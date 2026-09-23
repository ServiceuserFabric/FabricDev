# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Maps workspaces lakehouse table, notebook and shortcut dependencies
# 
# ***To get started attach a lakehouse from the workspace.***
# 
# Calling the main class <br>
# FlowScrapper: <br>
# - Scrapes the notebook content in workspace, supported read and write commandas are read. additional commands .table() and .saveAsTable(). other read and save methods won't display. Locations can be parametrised by simple variables or by f strings that have the variable inside {parameter}. saveAsTable(lakehouse + "." + target) or , separated strings are not supported!
# - Scrapes all tables and lakehouses in the workspace
# - Scrapes all shortcuts in lakehouses in the workspace, all shortcuts outside of onelake are called External. This might fail incase user running the notebook doesn't have access to all workspaces there are shortcuts into.
# - If notebook prefix list is specified, example FlowScraper(['bronze','silver','gold']) it will only look for notebooks that start with those names. 
# 
# method .visualize():
# - Creates a visualization of the scraped data.
# - If you want to change the styling of the picture just modify the _style() method
# 
# method .save_graph(filename):
# - This method loses all styling because XML doesn't support most of it. So the end result is black white picture with boxes.
# - Saves the graph into /lakehouse/default/Files/FlowScrapper/filename
# - The formats are png and XML. XML can be opened with Drawio or other diagraming tools for even better visualization.
# 
# method .create_dag():
# - this prints out DAG that can be used with notebookutils.notebook.runMultiple() based on the dependencies between notebooks. 
# 
# Example use: <br>
# model = FlowScraper() <br>
# model.visualize() <br>
# model.save_graph('test') <br>
# model.create_dag()
# 
# 
# the extra package is needed to create XML files. 


# CELL ********************

%conda install conda-forge::graphviz2drawio -y -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import re
import concurrent.futures
from graphviz2drawio import graphviz2drawio
import requests
import json
from graphviz import Digraph
from IPython.display import display
import networkx as nx
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

self_notebook_name=notebookutils.runtime.context.get('currentNotebookName')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## The main class

# CELL ********************

class FlowScrapper:

    
    

    def __init__(self,notebook_prefix=[]):
        self.errors = []
        self.self_notebook_name = notebookutils.runtime.context.get('currentNotebookName')
        self.notebook_prefix = notebook_prefix
        self.notebook_mapping = self._go_through_workspace()
        self.notebooks = self._create_directional_graph(self.notebook_mapping)
        self.shortcuts = self._get_shortcuts()
        self.tables = self._get_tables()
        self._incase_errors()
    
    def _incase_errors(self):
        if len(self.errors)>0:
            print("""*** Errros parsing notebook content ***\n
            user .errors to get error listing """)
    #Take only code of notebook
    #j_book.get('cells')

    def _parse_f_strings(self,f_string,whole_content):

        curly_bracket_match = r"(?<={).*?(?=})"
        #Remove the f" start from the string and ending ' or "
        content = f_string[2:-1]
        for variable in re.findall(curly_bracket_match,f_string):
            #
            print(variable)
            #print(whole_content)
            variable_match = r'%s[=\s]*["\'](.*?)["\']' % variable
            value = re.search(variable_match,whole_content)
            #print("matched. ")
            #print(value)
            value = value.group(0).split('=')[1].strip()
            value = value.replace('"','').replace("'",'').replace("\n","").replace("\\","")

            content = content.replace(r"{%s}" % variable,value)
            
            #print(content, " ", value)
        #print(f_string)
        #finally remove the curlies
        #content = content.replace("}","").replace("{","")
        return content

    def _parse_raw_variables(self,variable, whole_content):


        matching = r'%s[f=\s]*["\'](.*?)["\']' % variable
        #print(matching)
        #print(whole_content)
        org = re.search(matching, whole_content)
        result = org.group(0).split('=')[1].strip()
        #print(result)
        if ('f"' in result or "f'" in result):
            #print('Variable contains f string')
            result = self._parse_f_strings(result,whole_content)

        else:
            result = result.replace('"','').replace("'",'').replace("\n","").replace("\\","")

        return result

    def _notebook_scrapper_wrapper(self,notebook):
            notebook_content = notebookutils.notebook.getDefinition(notebook)
            node = self._notebook_scrapper(notebook_content,notebook)
            record = {
                "name": notebook,
                "type": "notebook",
                "dependencies": node
                }
            return record

    def _notebook_scrapper(self,notebook_content,notebook_name):
        j_book = json.loads(notebook_content)
        cells = j_book.get('cells')

        notebook_content = []
        for cell in cells:
            if cell.get('cell_type') == 'code':
                cell = cell['source']
                #Clean comments
                for row in cell: 
                    #Find the comment mark and remove everything from that row after comment: This will be in #this will not
                    comment_index = row.rfind('#')
                    if comment_index != -1:
                        row = row[:comment_index]
                    #Remove tabs
                    row = row.replace("\t","")
                    notebook_content.append(row)

        notebook_content = str(notebook_content)
        #print(notebook_content)
        # Regular expressions for spark.read and spark.write
        read_pattern = r'(?<=spark\.read\.).*?(?=,)'
        write_pattern = r'saveAsTable\((.*?)\)'

        # Find all matches
        read_matches_raw = re.findall(read_pattern, notebook_content)
        write_matches_raw = re.findall(write_pattern, notebook_content)
        #print(write_matches_raw)
        read_tables = []
        write_tables = []
        print(f'working on {notebook_name}')
        for raw in read_matches_raw:
            #print(raw)
            try:
                res = re.search('table\((.*?)\)',raw).group().replace("table(","").replace(')','')
            #print(res)
                try:
                    #handle basic variables
                    if ('"' not in res) and ("'" not in res):
                        #print(f"looking source for parmater {raw} in {notebook_name}")
                        read_tables.append(self._parse_raw_variables(res,notebook_content))
                    #handle f strings
                    elif ('f"' in res or "f'" in res) and "{" in res:
                        #print("looking for f string")
                        read_tables.append(self._parse_f_strings(res,notebook_content))
                    else:
                        table = res.replace('"','').replace("'","").replace("\\","")
                        read_tables.append(table)
                except Exception as e: #Handle errors incase something cannot be handled make it still visible, so it's easier to fix in drawio etc
                    print(f'cant parse read {notebook_name} to {raw}:')
                    self.errors.append(f'cant parse read {notebook_name} to {raw} param {res}: {e}')
                    write_tables.append(raw)
            except Exception as e:
                print(f'No reads found at {notebook_name}')

        for raw in write_matches_raw:
            try:
                #handle basic variables
                if ('"' not in raw) and ("'" not in raw):
                    print(f"looking source for parameter {raw} in {notebook_name}")
        
                    write_tables.append(self._parse_raw_variables(raw,notebook_content))
                #handle f strings
                elif ('f"' in raw or "f'" in raw) and "{" in raw: 
                    write_tables.append(self._parse_f_strings(raw,notebook_content))
                else:
                    raw = raw.replace('"','').replace("'","").replace("\\","")
                    write_tables.append(raw)
            except Exception as e:
               print(f'cant parse write of {notebook_name} to {raw}')
               self.errors.append(f'cant write read {notebook_name} to {raw}: {e}')
               write_tables.append(raw)

        dependency_tree_node = {
            'source' : read_tables,
            'target' : write_tables
        }
        return dependency_tree_node

    def _go_through_workspace(self):

        notebooks = notebookutils.notebook.list()
        if len(self.notebook_prefix)>0:
        # Filter notebooks based on prefixes
            filtered_notebooks = [notebook.get('displayName') for notebook in notebooks if any(notebook.get('displayName').startswith(prefix) for prefix in self.notebook_prefix)]
        else:
            filtered_notebooks = [notebook.get('displayName') for notebook in notebooks]

        #remove this notebook from list:
        if self.self_notebook_name in filtered_notebooks:
            filtered_notebooks.remove(self.self_notebook_name)

        print('Looking content of:')
        print(filtered_notebooks)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(self._notebook_scrapper_wrapper,notebook) for notebook in filtered_notebooks]

            #collect the data
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        return results
        # Make directionary graph from notebooks.

    def _create_directional_graph(self,notebook_mapping):
        rows = []
        for entry in notebook_mapping:

            row_notebook = (entry['name'] , [source.lower() for source in entry['dependencies']['source']],'notebook')
            rows.append(row_notebook)

            for table in entry['dependencies']['target']:
                target_table = (table.lower(), [entry['name']],'table')

                rows.append(target_table)

        return rows
        
    def _get_item_name(self,token,workspaceId,itemid):

        api_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{itemid}'

        #token = notebookutils.credentials.getToken('https://api.fabric.microsoft.com/')

        header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}
        resp = requests.get(url=api_url, headers = header)
        if resp.status_code == 429:
            time.sleep(5)
            header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}
            resp = requests.get(url=api_url, headers = header)
        if resp.status_code == 429:
            time.sleep(5)
            header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}
            resp = requests.get(url=api_url, headers = header)
        return resp.json().get('displayName')


    def _handle_shortcuts(self,shortcuts,lh_name,token):
        #workspaceId = notebookutils.runtime.context.get('currentWorkspaceId')
        # ('silver.dim_date', ['silver dim_date'], 'table')
        result = []
        for shortcut in shortcuts:
            target_table =  f"{lh_name}.{shortcut.get('name')}".lower()
            #source_lh_name = notebookutils.lakehouse.get(shortcut.get('target').get('oneLake').get('itemId')).get('displayName') #TODO add handling here incase other type of link!
            if shortcut.get('target').get('type') == 'OneLake':
                workspaceId = shortcut.get('target').get('oneLake').get('workspaceId')
                source_lh_name = self._get_item_name(token,workspaceId,shortcut.get('target').get('oneLake').get('itemId'))
                source_table_name = shortcut.get('target').get('oneLake').get('path').split('/',1)[1]
                source_table = f"{source_lh_name}.{source_table_name}".lower()
            else:
                source_table = "External source"
            edge_type = "shortcut_table"
            result.append((target_table,[source_table],edge_type))
        return result

    def _get_shortcuts(self):
        workspaceId = notebookutils.runtime.context.get('currentWorkspaceId')
        ret_array = []
        for lakehouse in notebookutils.lakehouse.list():
            name = lakehouse.get('displayName')
            #workspaceId = 
            lakehouseId = lakehouse.get('id')
            api_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/items/{lakehouseId}/shortcuts'
            
            token = notebookutils.credentials.getToken('https://api.fabric.microsoft.com/')

            header = {'Content-Type':'application/json','Authorization':f'Bearer {token}'}

            resp = requests.get(url=api_url, headers = header)
            shortcuts = resp.json().get('value')
            
            if len(shortcuts)>0:
                shortcut_tables = self._handle_shortcuts(shortcuts,name,token)
                ret_array += shortcut_tables
        return ret_array



    def _get_tables(self):
        ret_dic = {}
        for lakehouse in notebookutils.lakehouse.list():
            ret_arr = []
            lakehouse_name = lakehouse.get('displayName')
            for table in notebookutils.lakehouse.listTables(lakehouse_name):
                table_name = f"{lakehouse_name}.{table.get('name')}".lower()
                target_name = [lakehouse_name.lower()]
                edge_type = "lh_table"
                ret_arr.append((table_name,target_name,edge_type))
            
            ret_dic[lakehouse_name]= ret_arr
        return ret_dic


### Visualization


    def _style(self,label=None,style_flag=True):
        if style_flag:
            if label == 'notebook':
                return {"style":"filled,rounded","fillcolor":'#B2FF66',"shape":"tab"}
            elif label == 'table':
                return {}
            else: 
                return {}
        return {}

    def _style_edge(self,label=None):
       
        if label == 'notebook':
            return {}
        elif label == "shortcut_table":
            return {"style":"dashed"}
        else: 
            return {}
 

    def _create_graph(self,style_flag=True):
            #G = nx.DiGraph(comment)
        graph_size = '20!,20!'
        dpi = None
        dot = Digraph(comment='Dependencies between tables')
        dot.attr(rankdir="LR",  dpi=dpi)
        dot.attr('node', shape='box', fontsize='12')

        
        i = 0
        for lakehouse in self.tables:
            with dot.subgraph(name=f'cluster_{i}') as c:
                c.attr(style='filled,rounded,dashed',color='#BAD6F7')
                c.attr(label= lakehouse, headlabel = "True",fontsize='20')
                for table in self.tables.get(lakehouse):
                    c.node(table[0],**self._style(table[2],style_flag))
            i +=1

        shortcuts_and_notebooks = self.notebooks + self.shortcuts
            
        for entry in shortcuts_and_notebooks:
            for item in entry[1]:
                dot.edge(item,entry[0],**self._style_edge(entry[2]))


            dot.node(entry[0], **self._style(entry[2],style_flag))
        return dot


    def visualize(self):
        dot = self._create_graph(style_flag = True)
        display(dot)

    def save_graph(self,filename):
        graph = self._create_graph(style_flag = False)

        graph.render(format='png',filename= filename ,directory = "/lakehouse/default/Files/FlowScrapper")#,view=True)
        graph_to_convert = f'/lakehouse/default/Files/FlowScrapper/{filename}'
        xml = graphviz2drawio.convert(graph_to_convert)
        with open(f"/lakehouse/default/Files/FlowScrapper/{filename}.xml", "a") as f:
            f.write(xml)
        #make pretty picture
        graph = self._create_graph(style_flag = True)
        graph.render(format='png',filename= filename ,directory = "/lakehouse/default/Files/FlowScrapper")#,view=True)
        

    def create_dag(self):
        data = self.notebooks
        # Step 1: Filter out the records with the type 'notebook'
        notebooks = {item[0]: item[1] for item in data if item[2] == 'notebook'}
        tables = {item[0]: item[1] for item in data if item[2] == 'table'}

        # Step 2: Create a mapping of tables to their source notebooks
        table_to_notebook = {}
        for table, sources in tables.items():
            for source in sources:
                if source in notebooks:
                    table_to_notebook[table] = source


        # Step 3: Create a mapping of notebooks to their dependent tables
        notebook_dependencies = {notebook: [] for notebook in notebooks}
        for notebook, sources in notebooks.items():
            for source in sources:
                if source in table_to_notebook:
                    notebook_dependencies[notebook].append(table_to_notebook[source])

        # Step 4: Construct the DAG
        dag = []
        for notebook, dependencies in notebook_dependencies.items():
            dag.append({
                "name": notebook,
                "path": notebook,
                "timeoutPerCellInSeconds":6000,
                "args":{
                    "useRootDefaultLakehouse": True
                    },
                "retry" : 1,
                "retryIntervalSeconds": 60,
                "dependencies": dependencies
            })
        
        DAG={
            "activities": dag,
            "timeoutInSeconds": 3600, # max 1 hour for the entire pipeline
            "concurrency": 20 # max notebooks run in parallel
        }
        #Make it pretty and print it
        json_str = json.dumps(DAG,indent=4)
        json_str = json_str.replace('true','True')
        print(json_str)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initialize

# CELL ********************

# incase you don't want to check every notebook you can specify array containing all the prefixes that u want to chekc ['gold','silver']
model = FlowScrapper()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Visualization

# CELL ********************

# Visualizes the results
model.visualize()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save as drawio and png

# CELL ********************

#Saves the picture in xml format
model.save_graph('test')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## GET DAG

# CELL ********************

#Create a efficient dag to be used with run_multiple()
model.create_dag()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
