select 1, st_transform(ST_SetSRID(ST_MakePoint(3209361.5066096946,5945617.220976858),3857), 4326)
union all
select 2,st_transform(ST_SetSRID(ST_MakePoint(3209482.1337949573,5945737.848162121),3857), 4326)

3209361.5066096946,5945617.220976858,3209482.1337949573,5945737.848162121&


select st_transform(st_setsrid(st_makepoint(234714.97775758,214579.35062085), 4026), 4326)

https://moldova-map.md/geoserver/cadastru_data/wms?service=WMS&version=1.1.1&request=GetFeatureInfo&exceptions=application%2Fjson&id=UAT1_intravelan__6dd23230-77d7-11ee-a4f4-65d1f977992d&layers=UAT1_intravelan&query_layers=UAT1_intravelan&x=51&y=51&height=101&width=101&srs=EPSG:3857&bbox=3196007.002422055,5963055.134709868,3196972.0199041553,5964020.152191968&feature_count=10&info_format=application%2Fjson&ENV=mapstore_language:en 
 
https://moldova-map.md/geoserver/cadastru_data/wms?service=WMS&version=1.1.1&request=GetFeatureInfo&exceptions=application%2Fjson&id=UAT2__6d3296d0-77d7-11ee-a4f4-65d1f977992d&layers=UAT2&query_layers=UAT2&x=51&y=51&height=101&width=101&srs=EPSG:3857&bbox=3196007.002422055,5963055.134709868,3196972.0199041553,5964020.152191968&feature_count=10&info_format=application%2Fjson&ENV=mapstore_language:en 
 
https://geoportal.md/ro/tool/info/index/x/0/y/0/width/0/height/0/lon/234714.97775758/lat/214579.35062085/bbox/0,0,0,0/layers/ComuneV2,RaioaneV2,AdreseV2,CladiriV2,TerenuriV2,StraziV2,Localita

with localities as (    
	select osm_id, name, way --, (ST_DumpPoints(st_envelope(st_expand(way, 1300)))).geom as bbox
    from planet_osm_point    
	where place in ('city', 'town', 'village') 
	and osm_id = 360006780
),
bbox as (
    select osm_id, (ST_DumpPoints(st_envelope(st_expand(way, 1300)))).geom
    from localities
),
geoportal_urls as (
    select osm_id, st_x(st_setsrid(way, 4026)), st_y(st_setsrid(way, 4026))
    --'https://geoportal.md/ro/tool/info/index/x/0/y/0/width/0/height/0/lon/' || st_x(st_setsrid(way, 4026)) || '/lat/' ||  st_y(st_setsrid(way, 4026)) || '/bbox/0,0,0,0/layers/ComuneV2,RaioaneV2,AdreseV2,CladiriV2,TerenuriV2,StraziV2,Localita' as geoportal_url
    from localities
)
select * from geoportal_urls

 min(st_x(bbox)) as min_x, max(st_x(bbox)) as max_x, min(st_y(bbox)) as min_y, max(st_y(bbox)) as max_y
from localities
group by osm_id









with localities as (    
	select osm_id, name, way --, (ST_DumpPoints(st_envelope(st_expand(way, 1300)))).geom as bbox
    from planet_osm_point    
	where place in ('city', 'town', 'village') 
	--and osm_id = 360006780
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
select l.osm_id, l.name, g.url, m.url
from localities l
join geoportal_urls g on g.osm_id = l.osm_id
join moldova_map_urls m on m.osm_id = l.osm_id




select osm_id, 

    ST_Collect(
        ST_Collect(ST_Transform(geoportal_comuna, 4326), ST_Transform(geoportal_localitate, 4326)),
        ST_Collect(
            ST_Collect(ST_Transform(mdmap_uat1_intravelan, 4326), ST_Transform(mdmap_uat1, 4326)),
            ST_Transform(mdmap_uat2, 4326)
        )
    ),
from fetched_localities



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
