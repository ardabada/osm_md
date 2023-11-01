using Npgsql;

var query = @"
with localities as (    
	select osm_id, name, way
    from planet_osm_point    
	where place in ('city', 'town', 'village')
),
bbox as (
    select osm_id, (ST_DumpPoints(st_envelope(st_expand(st_transform(way, 3857), 170)))).geom as point
    from localities
),
geoportal_urls as (
    select osm_id,
    'https://geoportal.md/ro/tool/info/index/x/0/y/0/width/0/height/0/lon/' || st_x(st_transform(way, 4026)) || '/lat/' ||  st_y(st_transform(way, 4026)) || '/bbox/0,0,0,0/layers/ComuneV2,RaioaneV2,AdreseV2,CladiriV2,TerenuriV2,StraziV2,Localita' as url
    from localities
),
moldova_map_urls as (
	select osm_id,
	'https://moldova-map.md/geoserver/cadastru_data/wms?service=WMS&version=1.1.1&request=GetFeatureInfo&exceptions=application%2Fjson&layers=UAT1_intravelan,UAT1,UAT2&query_layers=UAT1_intravelan,UAT1,UAT2&x=51&y=51&height=101&width=101&srs=EPSG:3857&bbox=' || min(st_x(point)) || ',' || min(st_y(point)) || ',' ||  max(st_x(point)) || ',' || max(st_y(point)) || '&feature_count=10&info_format=application%2Fjson&ENV=mapstore_language:en' as url
    from bbox
    group by osm_id
)
select l.osm_id, l.name, g.url as geoportal, m.url  as moldova_map
from localities l
join geoportal_urls g on g.osm_id = l.osm_id
join moldova_map_urls m on m.osm_id = l.osm_id";

var rootDirectory = "..\\data";
var connectionString = "Host=localhost;Port=5432;Database=osmtest;Username=postgres;Password=postgres";

var data = new List<OsmData>();
using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
{
    connection.Open();

    using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
    {
        using (NpgsqlDataReader reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                data.Add(new OsmData
                {
                    OsmId = reader.GetInt64(0),
                    Name = reader.GetString(1),
                    GeoportalUrl = reader.GetString(2),
                    MoldovaMapUrl = reader.GetString(3)
                });
            }
        }
    }
}

Console.WriteLine("Data received from sql");

var options = new ParallelOptions
{
    MaxDegreeOfParallelism = Environment.ProcessorCount
};
var httpClient = new HttpClient();

await Parallel.ForEachAsync(data, options, async (osmData, token) =>
{
    try
    {
        var response = await httpClient.GetAsync(osmData.GeoportalUrl, token);
        var responseBody = await response.Content.ReadAsStringAsync(token);

        var dir = Path.Combine(rootDirectory, osmData.OsmId.ToString());
        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);

        File.WriteAllText(Path.Combine(dir, "geoportal.raw"), responseBody);

        response = await httpClient.GetAsync(osmData.MoldovaMapUrl, token);
        responseBody = await response.Content.ReadAsStringAsync(token);

        dir = Path.Combine(rootDirectory, osmData.OsmId.ToString());
        if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);

        File.WriteAllText(Path.Combine(dir, "moldovamap.raw"), responseBody);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error for id {osmData.OsmId}: {ex.Message}");
    }
});

Console.WriteLine("done");

class OsmData
{
    public long OsmId { get; set; }

    public string Name { get; set; }

    public string GeoportalUrl { get; set; }

    public string MoldovaMapUrl { get; set; }
}