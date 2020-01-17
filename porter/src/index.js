import { join } from 'path';

import { db, helpers } from './database';
import loggers from './logging';
import { readAllFiles } from './json';

const log = loggers('app');

const start = async () => {
  log.info('Reading JSON data');

  const vendorData = await readAllFiles(
    join(`${__dirname}`, '..', '..', 'json', 'vendors')
  );
  const flavorData = await readAllFiles(
    join(`${__dirname}`, '..', '..', 'json', 'flavors')
  );

  log.info('Parsing JSON data');

  const flavors = [];
  const vendors = new Map();

  for (const flavor of flavorData) {
    const { vendor } = flavor;

    if (flavor.rejected || !flavor.confirmed) {
      log.info(`Skipping unconfirmed flavor ${flavor.name}`);
      continue;
    }

    // only search out each vendor once
    if (!vendors.has(vendor.mix_sid)) {
      const fullVendor = vendorData.find(
        searched => searched.mix_sid === vendor.mix_sid
      );

      vendors.set(vendor.mix_sid, {
        identifier: vendor.mix_sid,
        code: fullVendor.abbreviation,
        ...fullVendor
      });
    }

    flavors.push({
      identifier: flavor.mix_sid,
      // eslint-disable-next-line camelcase
      vendor_identifier: flavor.vendor.mix_sid,
      ...flavor
    });
  }

  log.info('Inserting data into database');
  const startTime = process.hrtime();

  db.tx('bulk-insert', async t => {
    await t.none(
      `create temporary table new_vendor_temp
          (identifier text primary key, name varchar(200), slug varchar(200), code varchar(5))
        `
    );
    await t.none(
      `create temporary table new_flavor_temp
          (vendor_identifier varchar(200) default null, identifier text primary key, name varchar(200), slug varchar(200), density numeric(5,4))`
    );

    const vendorCols = new helpers.ColumnSet(
      ['identifier', 'name', 'slug', 'code'],
      {
        table: 'new_vendor_temp'
      }
    );
    const flavorCols = new helpers.ColumnSet(
      ['identifier', 'vendor_identifier', 'name', 'slug', 'density'],
      {
        table: 'new_flavor_temp'
      }
    );

    await t.none(helpers.insert(Array.from(vendors.values()), vendorCols));
    await t.none(helpers.insert(flavors, flavorCols));
    await t.none(`
      with s as (
        select * from new_vendor_temp
      ), update_vendor as (
        update vendor v
        set name = s.name, slug = s.slug, code = s.code
        from vendor_identifier vi
        join s on vi.identifier = s.identifier
        where vi.vendor_id = v.id
        returning vi.identifier
      ), insert_vendor as (
        insert into vendor (id, name, slug, code)
          select nextval('vendor_id_seq'), s.name, s.slug, s.code
          from s
          left join vendor v on
            s.name = v.name
            and s.code = v.code
          where
            v.id is null
            and s.identifier not in (select identifier from update_vendor)
        returning currval('vendor_id_seq') id, name, slug, code
      )
      insert into vendor_identifier (vendor_id, data_supplier_id, identifier)
        select coalesce(v.id, iv.id), 2, nvt.identifier
        from new_vendor_temp nvt
          left join vendor_identifier vi on
            nvt.identifier = vi.identifier
            and vi.data_supplier_id = 2
          left join vendor v on
            nvt.name = v.name
            and nvt.code = v.code
          left join insert_vendor iv on
            nvt.name = iv.name
            and nvt.code = iv.code
          where
            (v.id is not null
            or iv.id is not null)
            and vi.identifier is null
      `);
    await t.none(`
      with s as (
        select
          row_number() over (partition by vi.vendor_id, nft.name) ordinal,
          nft.*,
          vi.vendor_id
        from new_flavor_temp nft
        join vendor_identifier vi
          on vi.data_supplier_id = 2
          and nft.vendor_identifier = vi.identifier
      ), update_flavor as (
        update flavor f
        set name = s.name, slug = s.slug, density = s.density
        from flavor_identifier fi
        join s on fi.identifier = s.identifier
        where fi.flavor_id = f.id
        returning fi.identifier
      ), insert_flavor as (
        insert into flavor (id, vendor_id, name, slug, density)
          select nextval('flavor_id_seq'), so.vendor_id, so.name, so.slug, so.density
          from (
            select *
            from s
            where
              s.ordinal = 1
              and s.identifier not in (select identifier from update_flavor)
          ) so
          left join flavor f on so.vendor_id = f.vendor_id and so.name = f.name
          where f.id is null
        on conflict do nothing
        returning currval('flavor_id_seq') id, vendor_id, name, slug, density
      )
      insert into flavor_identifier (flavor_id, data_supplier_id, identifier)
        select coalesce(f.id, if.id), 2, s.identifier
        from s
        left join flavor f on
          s.vendor_id = f.vendor_id
          and s.name = f.name
        left join insert_flavor if on
          s.vendor_id = if.vendor_id
          and s.name = if.name
        left join flavor_identifier fi on
          (s.identifier = fi.identifier
          or if.id = fi.flavor_id)
          and fi.data_supplier_id = 2
        where
          (f.id is not null
          or if.id is not null)
          and s.ordinal = 1
          and fi.identifier is null
      `);

    const endTime = process.hrtime(startTime);
    // add nanos to full seconds
    const duration = endTime[0] + endTime[1] / 1e6;
    const { count: vendorCount } = await t.one(
      'select count(*) from new_vendor_temp'
    );
    const { count: flavorCount } = await t.one(
      'select count(*) from new_flavor_temp'
    );

    const rate =
      ((parseInt(vendorCount, 10) + parseInt(flavorCount, 10)) / duration) *
      1e3;

    log.info(
      `Inserts took ${duration.toFixed(2)}ms (${Math.floor(rate)} rows/sec)`
    );
    log.info(`Inserted ${vendorCount} vendors and ${flavorCount} flavors`);
  })
    .then(events => {
      log.info(events);
    })
    .catch(error => {
      log.error(`Error: ${error.message}`);
      log.error(`${error.code} - ${error.detail}`);
    });
};

start();
