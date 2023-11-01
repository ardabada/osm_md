using System.Globalization;
using Newtonsoft.Json.Linq;
using Npgsql;

var connectionString = "Host=localhost;Port=5432;Database=osmtest;Username=postgres;Password=postgres";
var rootDirectory = "..\\data";

var k = GetSqlData("10162211876", rootDirectory);
return;

var dirs = Directory.GetDirectories(rootDirectory);
var localities = new List<FetchedLocality>();
foreach (var dir in dirs)
{
    var id = Path.GetFileName(dir);
    localities.Add(GetSqlData(id, rootDirectory));
}

Console.WriteLine(localities.Count);

//var locality = GetSqlData("360006780", rootDirectory);

return;
using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
{
    connection.Open();

    foreach (var locality in localities)
    {
        var query = $"INSERT INTO fetched_localities(osm_id, geoportal_comuna, geoportal_localitate, mdmap_uat1_intravelan, mdmap_uat1, mdmap_uat2) VALUES({locality.OsmId}, {locality.geoportal_comuna}, {locality.geoportal_localitate}, {locality.mdmap_uat1_intravelan}, {locality.mdmap_uat1},  {locality.mdmap_uat2})";

        /*
         * osm_id bigint,
        geoportal_comuna geometry,
        geoportal_localitate geometry,
        mdmap_uat1_intravelan geometry,
        mdmap_uat1 geometry,
        mdmap_uat2 geometry
         * */

        using (NpgsqlCommand command = new NpgsqlCommand(query, connection))
        {
            command.ExecuteNonQuery();
        }
    }
}

Console.WriteLine("done");

static FetchedLocality GetSqlData(string id, string rootDirectory)
{
    long osmId = long.Parse(id);

    FetchedLocality result = new FetchedLocality { OsmId = osmId };

    JToken geoPortalJson = JToken.Parse(File.ReadAllText(Path.Combine(rootDirectory, id, "geoportal.raw")));
    foreach (var record in geoPortalJson["records"])
    {
        if (record["html"].ToString().Equals("comune", StringComparison.InvariantCultureIgnoreCase))
        {
            result.geoportal_comuna = "ST_setsrid(ST_GeomFromText('" + record["children"].ToArray()[0]["the_geom"].ToString() + "'), 4026)";
        }
        else if (record["html"].ToString().Equals("Localități (hotare neaprobate)", StringComparison.InvariantCultureIgnoreCase))
        {
            result.geoportal_localitate = "ST_setsrid(ST_GeomFromText('" + record["children"].ToArray()[0]["the_geom"].ToString() + "'), 4026)";
        }
    }

    JToken moldovaMapJson = JToken.Parse(File.ReadAllText(Path.Combine(rootDirectory, id, "moldovamap.raw")));
    foreach (var feature in moldovaMapJson["features"])
    {
        if (feature["id"].ToString().StartsWith("UAT1_intravelan"))
        {
            result.mdmap_uat1_intravelan = Feature.Parse(feature).Geometry;
        }
        else if (feature["id"].ToString().StartsWith("UAT1."))
        {
            result.mdmap_uat1 = Feature.Parse(feature).Geometry;
        }
        else if (feature["id"].ToString().StartsWith("UAT2."))
        {
            result.mdmap_uat2 = Feature.Parse(feature).Geometry;
        }
    }

    return result;
}

class Feature
{
    private Feature(string text)
    {
        Shape = text;
    }

    public string Shape { get; }

    public string Geometry => "ST_SetSRID(ST_GeomFromText('" + Shape  + "'), 3857)";

    public static Feature Parse(JToken feature)
    {
        if (feature["type"].ToString() != "Feature") throw new InvalidOperationException();

        var coords = feature["geometry"]["coordinates"];
        var geometryType = feature["geometry"]["type"].ToString();

        if (geometryType == "Polygon")
        {
            var text = string.Join(",", coords.ToArray()[0].ToArray().Select(x => x[0].ToObject<double>().ToString(CultureInfo.InvariantCulture) + " " + x[1].ToObject<double>().ToString(CultureInfo.InvariantCulture)));

            return new Feature("POLYGON((" + text + "))");
        }
        else if (geometryType == "MultiPolygon")
        {
            var text = string.Join(",", coords.ToArray().Select(x => "(" + string.Join(",", x.ToArray()[0].ToArray().Select(y => y[0].ToObject<double>().ToString(CultureInfo.InvariantCulture) + " " + y[1].ToObject<double>().ToString(CultureInfo.InvariantCulture))) + ")"));

            return new Feature("MULTIPOLYGON((" + text + "))");

        }
        else throw new InvalidOperationException();
    }
}

class FetchedLocality
{
    public long OsmId { get; set; }

    public string geoportal_comuna { get; set; } = "NULL";

    public string geoportal_localitate { get; set; } = "NULL";

    public string mdmap_uat1_intravelan { get; set; } = "NULL";

    public string mdmap_uat1 { get; set; } = "NULL";

    public string mdmap_uat2 { get; set; } = "NULL";
}


/*
 
-- create table fetched_localities(
-- 	osm_id bigint,
-- 	geoportal_comuna geometry,
-- 	geoportal_localitate geometry,
-- 	mdmap_uat1_intravelan geometry,
-- 	mdmap_uat1 geometry,
-- 	mdmap_uat2 geometry
-- );


with localities as (    
	select osm_id, name, way
    from planet_osm_point    
	where place in ('city', 'town', 'village')
)

--select fl.osm_id, st_transform(l.way, 4326) from fetched_localities fl join localities l on l.osm_id = fl.osm_id where fl.mdmap_uat1_intravelan is null and fl.geoportal_localitate is null

select fl.osm_id, l.name,
    ST_Collect(ST_Collect(ST_Transform(fl.geoportal_localitate, 4326), ST_Transform(st_makevalid(fl.mdmap_uat1_intravelan), 4326)), ST_Transform(l.way, 4326))
from fetched_localities fl
join localities l on fl.osm_id = l.osm_id

 */