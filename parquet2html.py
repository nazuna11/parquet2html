from fastparquet import ParquetFile
import json

def makeHtmlFrame(table_elements):
    return f"""
    <!doctype html>
    <html>
    <head>
    <meta charset="utf-8">
    <title>parquet2html</title>
    <!--<link href="css/style.css" rel="stylesheet" type="text/css">-->
    </head>
    <body>
    {"".join(table_elements)}
    </body>
    </html>
    """

def createListHtml(cname, ctype):
    return f"""<tr>
      <td>{cname}</td>
      <td>{str(ctype)}</td>
    </tr>"""

def createTableHtml(fields, title, result_array=[]):
    columns = []
    for field in fields["fields"]:
        if isinstance(field["type"], dict) and field["type"]["type"]=="struct":
            createTableHtml(field["type"], field["name"], result_array)
            column_type = "struct"
        elif isinstance(field["type"], dict) and field["type"]["type"]=="array":
            array_type = field["type"]["elementType"]

            if isinstance(array_type, dict) and array_type["type"] == "struct":
                array_type = "struct"
                createTableHtml(field["type"]["elementType"], field["name"], result_array)
            column_type = f"array<{array_type}>"
        else:
            column_type = field["type"]
        column_html = createListHtml(field["name"], column_type)
        columns.append(column_html)

    result_array.append(f"""
        <div id="info">
        <h3>{title}</h3>
        <table border="1">
            <tr>
              <th>neme</th>
              <th>type</th>
            </tr>
            {"".join(columns)}
        </table>
        </div>
        """)

table = pf = ParquetFile('data/parquet/part-00000-4b0ffe1c-29e3-42c1-881a-686d08da8e37-c000.snappy.parquet')
parent_schema = table.key_value_metadata["org.apache.spark.sql.parquet.row.metadata"]

parent_fields = json.loads(parent_schema)
result_array = []
result = createTableHtml(parent_fields, "root", result_array)
result_array.reverse()

html_str = makeHtmlFrame(result_array)

with open("index.html", "w") as f:
    f.write(html_str)
