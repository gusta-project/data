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
          select nextval('vendor_id_seq'), name, slug, code
          from s
          where s.identifier not in (select identifier from update_vendor)
        returning currval('vendor_id_seq') id, name, slug, code
      )
      insert into vendor_identifier (vendor_id, data_supplier_id, identifier)
        select iv.id, 2, nvt.identifier
        from new_vendor_temp nvt
          join insert_vendor iv on
            nvt.name = iv.name
            and nvt.slug = iv.slug
            and nvt.code = iv.code
      `);
    await t.none(`
      with s as (
        select nft.*, vi.vendor_id
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
          select nextval('flavor_id_seq'), vendor_id, name, slug, density
          from s
          where s.identifier not in (select identifier from update_flavor)
        returning currval('flavor_id_seq') id, vendor_id, name, slug, density
      )
      insert into flavor_identifier (flavor_id, data_supplier_id, identifier)
        select if.id, 2, nft.identifier
        from new_flavor_temp nft
          join insert_flavor if on
            nft.name = if.name
            and nft.slug = if.slug
            and nft.density = if.density
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
