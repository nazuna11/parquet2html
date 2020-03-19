__author__ = "nazuna <nazuna11develop@gmail.com>"
__status__ = "production"
__version__ = "0.0.1"
__date__    = "20 March 2020"

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

def createListHtml(cname, ctype, struct_flag):
    if struct_flag:
        return f"""<tr>
          <td>{cname}</td>
          <td><p><a href="#{cname}">{str(ctype)}</a></p></td>
        </tr>"""

    return f"""<tr>
      <td>{cname}</td>
      <td>{str(ctype)}</td>
    </tr>"""

def createTableHtml(fields, title, result_array=[]):
    columns = []
    for field in fields["fields"]:
        struct_flag = False
        if isinstance(field["type"], dict) and field["type"]["type"]=="struct":
            createTableHtml(field["type"], field["name"], result_array)
            struct_flag = True
            column_type = "struct"
        elif isinstance(field["type"], dict) and field["type"]["type"]=="array":
            array_type = field["type"]["elementType"]

            if isinstance(array_type, dict) and array_type["type"] == "struct":
                array_type = "struct"
                createTableHtml(field["type"]["elementType"], field["name"], result_array)
                struct_flag = True
            column_type = f"""array&lt;{array_type}&gt;"""
        else:
            column_type = field["type"]
        column_html = createListHtml(field["name"], column_type, struct_flag)
        columns.append(column_html)

    result_array.append(f"""
        <h3 id="{title}">{title}</h3>
        <table border="1">
            <tr>
              <th>neme</th>
              <th>type</th>
            </tr>
            {"".join(columns)}
        </table>
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
