import csv
import json
import requests

# layers:
# # areas that are rivers.
# river_poly_50k = 50328
# # areas that are lakes
# lake_poly_50k = 50293
# # tracts of land covered by native trees
# native_poly_50k = 50293
# exotic50k = 50267
# nztreepointsNONpoly = 50365
# nzScrub50kpoly = 50339
# # each polygon describes a land in terms of five characteristics 
# nzLRIVegetation = 48055
# scattered_scrub_poly_50k = 50337
# sand_poly_50k = 50335
# # special soil profiling one:
# FSL_soil_drainage_50k = 48104
# # below is 50 poly unless stated otherwise.
# # crushed rock > 2mm size
# shingle = 50342
# orchard = 50307
# waterfall = 50372
# ice = 50287
# snow = 50352
# mud = 50305

layers = {
    "raster": {
        "elevation": 1418,
    },
    "polygon": {
        "elevation": 1418,
        "river": 50328,
        "lake": 50293,
        "native": 50293,
        "exotic": 50267,
        "scrub": 50339,
        "LRIvegetation": 48055,
        "sand": 50335,
        "FSLsoil": 48104,
        "shingle": 50342,
        "orchard": 50307,
        "waterfall": 50372,
        "ice": 50287,
        "snow": 50352,
        "mud": 50305
    },
    "points": {}
}

elev_layer_id = '1418'
api_key = "5aca10f6e3aa4041832d4a3bb3c33e9d"
url = "https://koordinates.com/services/query/v1/{type}.json?key={key}&layer={layer}&x={x}&y={y}"


with open('./input/01-Gavin-Water-Meta.tsv', 'r') as input_file:
    with open('./output/enriched_meta.tsv', 'w') as output_file:
        reader = csv.reader(input_file, delimiter="\t")
        writer = csv.writer(output_file, delimiter="\t")
        fieldnames = ["site", "x", "y"]
        for layer_type in layers:
            for layer in layers[layer_type]:
                fieldnames.append(layer)
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        next(reader) 
        for line in reader:
            print("----" + line[0] + "----")
            # make data linz api url here.
            # koordinates raster query.
            # wgs84 crs
            # api key required
            # layer id
            # callback (optional)
            # using NZ 80m digital
            polygon_layers = layers["polygon"]
            line_dict = {}
            for layer in polygon_layers:
                layer_id = polygon_layers[layer]
                layer_type = "vector"
                if layer == "elevation":
                    layer_type = "raster"
                params = {
                    "key": api_key,
                    "type": layer_type,
                    "layer": layer_id,
                    "x": line[1],
                    "y": line[2],
                }
                line_dict['site'] = line[0]
                line_dict['x'] = line[1]
                line_dict['y'] = line[2]
                r = requests.get(url.format(**params))
                if r.status_code == 429:
                    print("api limit exceeded")
                else:
                    data = r.json()
                    if layer == "elevation":
                        elev = data["rasterQuery"]["layers"][str(layer_id)]["bands"][0]["value"]
                        print('elevation: ' + str(elev))
                        line_dict['elevation'] = elev
                    else: 
                        if len(data["vectorQuery"]["layers"][str(layer_id)]["features"]) > 0:
                            print("%s: YES" % layer) 
                            line_dict[layer] = 1
                        else :
                            print("%s: NO" % layer) 
                            line_dict[layer] = 0
            writer.writerow(line_dict)
            # elev = data["rasterQuery"]["layers"][layer_id]["bands"][0]["value"]
            # new_line = line
            # new_line.append(elev)
            # print(new_line)
            # writer.writerow(new_line)