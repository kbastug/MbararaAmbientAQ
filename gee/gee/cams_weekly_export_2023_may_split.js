// gee/cams_weekly_export_2023_may_split.js
// Purpose: Export weekly PM2.5 for May 2023 (Mbarara point) using smaller chunks
// to work around intermittent Earth Engine internal errors (code 13) seen in early May 2023.
//
// Dataset: ECMWF/CAMS/NRT
// Band: particulate_matter_d_less_than_25_um_surface (kg/m^3)
// Conversion: kg/m^3 * 1e9 = µg/m^3
//
// Expected workflow:
// 1) Run each Export task from the GEE Tasks tab.
// 2) Download CSVs from Drive.
// 3) Combine/append with the rest of your 2023/Q2 exports (April + June, etc).
//
// Note: We’re exporting May 2023 in multiple chunks:
//   - May 01–08
//   - May 08–15
//   - May 15–22
//   - May 22–Jun 01
//
// If any ONE of these still fails, use the daily fallback block at the bottom
// for that specific week and compute the weekly mean in Excel.

var pt = ee.Geometry.Point([30.65, -0.61]);   // Mbarara town approx
var scaleMeters = 45000;                      // CAMS ~44 km grid

function weeklyTable(startDate, endDate) {
  startDate = ee.Date(startDate);
  endDate   = ee.Date(endDate);

  var camsDaily = ee.ImageCollection('ECMWF/CAMS/NRT')
    .filterDate(startDate, endDate)
    .filter('model_initialization_hour == 0')
    .filter('model_forecast_hour == 0')
    .select('particulate_matter_d_less_than_25_um_surface')
    .map(function(img) {
      return img.multiply(1e9).rename('pm25_ugm3')
        .copyProperties(img, ['system:time_start']);
    });

  // Number of full weeks between dates
  var nWeeks = endDate.difference(startDate, 'week').floor();
  var weeks = ee.List.sequence(0, nWeeks.subtract(1));

  var weekly = ee.FeatureCollection(weeks.map(function(w) {
    w = ee.Number(w);
    var ws = startDate.advance(w, 'week');
    var we = ws.advance(1, 'week');

    var wkMeanImg = camsDaily.filterDate(ws, we).mean();

    var pm = wkMeanImg.reduceRegion({
      reducer: ee.Reducer.mean(),
      geometry: pt,
      scale: scaleMeters,
      bestEffort: true,
      maxPixels: 1e13
    }).get('pm25_ugm3');

    return ee.Feature(null)
      .set('week_start', ws.format('YYYY-MM-dd'))
      .set('week_end', we.advance(-1, 'day').format('YYYY-MM-dd'))
      .set('pm25_ugm3', pm);
  })).filter(ee.Filter.notNull(['pm25_ugm3']));

  return weekly;
}

// ---- May 2023 weekly exports (chunked) ----
// These should each produce ~1 row (one week), except the last chunk can produce 1–2 depending on alignment.

Export.table.toDrive({
  collection: weeklyTable('2023-05-01', '2023-05-08'),
  description: 'Mbarara_CAMS_Weekly_PM25_2023_05_Week1',
  fileFormat: 'CSV'
});

Export.table.toDrive({
  collection: weeklyTable('2023-05-08', '2023-05-15'),
  description: 'Mbarara_CAMS_Weekly_PM25_2023_05_Week2',
  fileFormat: 'CSV'
});

Export.table.toDrive({
  collection: weeklyTable('2023-05-15', '2023-05-22'),
  description: 'Mbarara_CAMS_Weekly_PM25_2023_05_Week3',
  fileFormat: 'CSV'
});

Export.table.toDrive({
  collection: weeklyTable('2023-05-22', '2023-06-01'),
  description: 'Mbarara_CAMS_Weekly_PM25_2023_05_Week4plus',
  fileFormat: 'CSV'
});


// -------------------------------------------------------------------
// OPTIONAL FALLBACK (only use if a specific week export still fails):
// Export daily values for that week and compute the mean in Excel.
//
// How:
// 1) Uncomment the block below.
// 2) Set startDaily/endDaily to the failing week.
// 3) Run the single export task.
// 4) In Excel: average the pm25_ugm3 column for those 7 days.
// -------------------------------------------------------------------

// var startDaily = ee.Date('2023-05-01'); // <-- set to failing week start
// var endDaily   = ee.Date('2023-05-08'); // <-- set to failing week end
//
// var camsDailyWeek = ee.ImageCollection('ECMWF/CAMS/NRT')
//   .filterDate(startDaily, endDaily)
//   .filter('model_initialization_hour == 0')
//   .filter('model_forecast_hour == 0')
//   .select('particulate_matter_d_less_than_25_um_surface')
//   .map(function(img) {
//     return img.multiply(1e9).rename('pm25_ugm3')
//       .copyProperties(img, ['system:time_start']);
//   });
//
// var dailyTable = ee.FeatureCollection(camsDailyWeek.map(function(img) {
//   var pm = img.reduceRegion({
//     reducer: ee.Reducer.mean(),
//     geometry: pt,
//     scale: scaleMeters,
//     bestEffort: true,
//     maxPixels: 1e13
//   }).get('pm25_ugm3');
//
//   return ee.Feature(null)
//     .set('date', ee.Date(img.get('system:time_start')).format('YYYY-MM-dd'))
//     .set('pm25_ugm3', pm);
// })).filter(ee.Filter.notNull(['pm25_ugm3']));
//
// Export.table.toDrive({
//   collection: dailyTable,
//   description: 'Mbarara_CAMS_Daily_PM25_2023_05_FallbackWeek',
//   fileFormat: 'CSV'
// });
