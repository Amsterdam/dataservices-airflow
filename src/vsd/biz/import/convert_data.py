import argparse
import math
import re

import pandas


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sql_shape", type=str, help="SQL from Shape file")
    parser.add_argument("xlsx", type=str, help="XLSX spreadsheet")
    parser.add_argument("output", type=str, help="output file")
    args = parser.parse_args()

    try:
        # Read Excel sheet in dataframe df
        df = pandas.read_excel(args.xlsx)

        # Read SQL data  in shape_map dictionary
        sql_file = open(args.sql_shape)
        sql = sql_file.readlines()
        shape_map = dict()
        for line in sql:
            m = re.search(
                r'INSERT INTO "public"\."bizzones" \("wkb_geometry" , "id1", "naam", "aktief", "ddingang".*\) '
                r"VALUES \(\'([^\']+)\', (\d+), \'([^\']+)\', \'([^\']+)\', \'([^\']+)\'",
                line,
            )
            if m:
                geometry, id1, naam, aktief, ddingang = (
                    m.group(1),
                    m.group(2),
                    m.group(3),
                    m.group(4),
                    m.group(5),
                )
                if aktief == "Ja":
                    shape_map[naam] = (geometry, id1, ddingang, naam)

        # l = list(shape_map.keys())
        # l.sort()
        # print("\n".join(l) )

        # Add name mapping. This maps the name in the spreadsheet to the name in the shape file

        name_mapping = {
            "A.J. Ernststraat": "AJ Ernststraat",
            "Albert Cuyp": "AlbertCuypstraat",
            "Albert Cuyp west": "Albert Cuypstraat West",
            "Bedrijvencentrum Osdorp": "BC Osdorp",
            "Centrum Nieuw West": "Osdorp Centrum",
            "Eerste van der Helststraat": "1e vd Helststraat",
            "Eerste van Swindenstraat": "1e van Swindenstraat",
            "Frans Hals Buurt": "Frans Halsstraat",
            "Gerard Doustraat en -plein": "Gerard Doustraat",
            "Haarlemmerbuurt 2e termijn": "Haarlemmerstraat",
            "Haven IJburg": "IJburg haven",
            "Hoofddorpplein e.o.": "Hoofddorppleinbuurt",
            "Jan Evertsenstraat e.o": "Jan Evertsenstraat",
            "Jodenbreestraat-Antoniesbreestraat": "Jodenbreestraat",
            "Kalverstraat-Heiligeweg eigenaren": "Kalverstraat",
            "Kalverstraat-Heiligeweg Gebruikers": "Kalverstraat",
            "Knowledge Mile": "Wibautstraat",
            "Middenweg e.o.": "Middenweg",
            "Molsteeg / Leliegracht e.o.": "Molsteeg EO",
            "Nieuwezijds Voorburgwal": "NZ Voorburgwal",
            "Olympiapleinbuurt": "Olympiaplein",
            "Oostelijke Eilanden & Czaar Peterbuurt": "Czaar Peterstraat",
            "Osdorp Centrum": "OsdorpCentrum",
            "Osdorper Ban eigenaren": "OsdorperbanEigenaar",
            "Plein '40-'45, Slotermeerlaan en Burgemeester de Vlugtlaan": "Plein 40 45",
            "Prinsheerlijk": "PrinsHeerlijk",
            "Rembrandtplein/Thorbeckeplein": "Rembrandtplein",
            "Rieker Businesspark": "Rieker Business Park",
            "Rokin eigenaren": "Rokin",
            "Rokin Gebruikers": "Rokin",
            "Rozengracht": "Rozengracht",
            "Spuibuurt": "Spuistraat",
            "The Olympic": "Stadionplein",
            "Uitgaansgebied Leidsebuurt": "Leidseplein",
            "Van Dam tot Westertoren": "Raadhuisstraat",
            "Vijzelstraat en Vijzelgracht ": "Vijzelstraat",
            "Warmoesstraat en omgeving": "Warmoesstraat",
        }

        def find_geometry(name):
            m_name = name_mapping[name] if name in name_mapping else name
            return shape_map[m_name][0] if m_name in shape_map else None

        df["geometry"] = df["Naam BIZ"].apply(find_geometry)

        # df1 = df[['Naam BIZ', 'geometry']]
        # df1[df1['geometry'].isnull()]

        def makequotedlink(s):
            s = s.strip("#")
            regex = re.compile(
                r"^(?:(?:http)s?://)?"  # http:// or https://
                r"(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)"  # domain...
                r"(?:/?|[/?]\S+)$",
                re.IGNORECASE,
            )
            if not re.match(regex, s):
                print(f"Invalid URL: {s}")
                return ""

            if not re.match("^https?://", s):
                s = "http://" + s
            return "'" + s + "'"

        def makesrid28992(s):
            return "ST_SetSRID('" + s + "'::geometry, 28992)"

        def make_insert(t):
            insert = """insert into biz_data_new(
          biz_id
        , naam
        , biz_type
        , heffingsgrondslag
        , website
        , heffing
        , bijdrageplichtigen
        , verordening
        , wkb_geometry)
        values (
          {}
        , '{}'
        , '{}'
        , '{}'
        , {}
        , {}
        , {}
        , {}
        , {}
        );
        """.format(
                t[0],
                t[1].replace("'", "''"),
                t[2],
                t[3],
                "NULL" if isinstance(t[4], float) and math.isnan(t[4]) else makequotedlink(t[4]),
                "NULL" if math.isnan(t[5]) else int(t[5]),
                "NULL" if math.isnan(t[6]) else int(t[6]),
                makequotedlink(t[7]),
                "NULL" if t[8] is None else makesrid28992(t[8]),
            )
            return insert

        inserts = []
        for t1 in df.itertuples():
            inserts.append(make_insert(t1))

        # Write file
        with open(args.output, "w") as f:
            f.write("\n".join(inserts))

    except OSError as exc:
        print(exc)


if __name__ == "__main__":
    main()
