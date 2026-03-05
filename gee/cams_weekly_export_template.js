// Weekly PM2.5 export for Mbarara using CAMS NRT

var pt = ee.Geometry.Point([30.65, -0.61]);
var scaleMeters = 45000;

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

  var nWeeks = endDate.difference(startDate, 'week').floor();
  var weeks = ee.List.sequence(0, nWeeks.subtract(1));

  return ee.FeatureCollection(weeks.map(function(w) {
    w = ee.Number(w);
    var ws = startDate.advance(w, 'week');
    var we = ws.advance(1, 'week');

    var pm = camsDaily.filterDate(ws, we).mean().reduceRegion({
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
}

Export.table.toDrive({
  collection: weeklyTable('2023-01-01','2023-04-01'),
  description: 'Mbarara_CAMS_Weekly_PM25_2023_Q1',
  fileFormat: 'CSV'
});
