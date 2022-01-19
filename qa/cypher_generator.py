#https://repl.it/@csga/yizhennciquery
from flask import Flask

# icdc default output schema
icdc_schema=["case_id","study_code","program","study_type",
    "breed","diagnosis","stage_of_disease","disease_site","age","gender","neutered_status","data_type","file_formats","files","samples"]

# icdc query parts
icdc_query={
  "part1":"MATCH (s:study) WITH COLLECT(DISTINCT(s.clinical_study_designation)) AS all_studies MATCH (d:demographic) WITH COLLECT(DISTINCT(d.breed)) AS all_breeds, COLLECT(DISTINCT(d.sex)) AS all_sexes, all_studies MATCH (d:diagnosis) WITH COLLECT(DISTINCT(d.disease_term)) AS all_diseases, all_breeds, all_sexes, all_studies MATCH (p:program)<-[*]-(s:study)<-[*]-(c:case)<--(demo:demographic), (c)<--(diag:diagnosis)",
  "part2":"OPTIONAL MATCH (f:file)-[*]->(c), (samp:sample)-[*]->(c) WITH DISTINCT c AS c, p, s, demo, diag, f, samp ",
  "return":{
    "case_id":"coalesce(c.case_id,'') AS case_id",
    "study_code":"coalesce(s.clinical_study_designation,'')  AS study_code",
    "program":"coalesce(p.program_acronym,'') AS program",
    "study_type":"coalesce(s.clinical_study_type,'') AS study_type",
    "breed":"coalesce(demo.breed,'') AS breed",
    "diagnosis":"coalesce(diag.disease_term,'') AS diagnosis",
    "stage_of_disease":"coalesce(diag.stage_of_disease,'') AS stage_of_disease",
    "disease_site":"coalesce(diag.primary_disease_site,'') AS disease_site",
    "age":"coalesce(demo.patient_age_at_enrollment,'') AS age",
    "gender":"coalesce(demo.sex,'') AS sex",
    "neutered_status":"coalesce(demo.neutered_indicator,'') AS neutered_status",
    "data_type":"COLLECT(DISTINCT(f.file_type)) AS data_types",
    "file_formats":"COLLECT(DISTINCT(f.file_format)) AS file_formats",
    "files":"COLLECT(DISTINCT(f)) AS files",
    "samples":"COLLECT(DISTINCT(samp.sample_id)) as samples",
    "number_of_study":"count(DISTINCT(s.clinical_study_designation)) as number_of_study",
    "number_of_cases":"count(DISTINCT(c.case_id)) as number_of_cases",
    "number_of_sample":"count(DISTINCT(samp)) as number_of_sample",
    "number_of_files":"count(DISTINCT(f)) as number_of_files"
    },
  "condition1":{
      "study_code":"s.clinical_study_designation IN  @@@  ",
      "breed":"demo.breed IN @@@",
      "diagnosis":"diag.disease_term IN @@@",
      "study_type":"s.clinical_study_type IN @@@",
      "disease_site":"diag.primary_disease_site  IN @@@",
      "stage_of_disease":"diag.stage_of_disease IN @@@",
      "gender":"demo.sex IN @@@",

  },
  "condition2":{
       "data_type": "f.file_type IN @@@",
       "file_formats": "f.file_format IN @@@",
   }
}

ctdc_schema=["case_id","clinical_trial_code","arm_id","arm_drug","pubmed_id","disease","gender","race","ethnicity","clinical_trial_id","trial_arm","file_types","file_formats","files"]

ctdc_query={
    "part1": "MATCH (t:clinical_trial)<--(a:arm)<--(c:case)<--(s:specimen)<--(:assignment_report) WITH DISTINCT c AS c, t ,a, s   ",
    "part2":"OPTIONAL MATCH (s)<-[*]-(f:file)",
    "return":{
        "case_id":"coalesce(c.case_id,'') AS case_id",
        "clinical_trial_code":"coalesce(t.clinical_trial_designation ,'')as clinical_trial_code",
        "arm_id":"coalesce(a.arm_id,'') As arm_id",
        "arm_drug":"coalesce(a.arm_drug,'') As arm_drug",
        "pubmed_id":"coalesce(a.pubmed_id,'') As pubmed_id",
        "disease":"coalesce(c.disease,'') As disease",
        "gender":"coalesce(c.gender,'') As gender",
        "race":"coalesce(c.race,'') As race",
        "ethnicity":"coalesce(c.ethnicity,'') As ethnicity",
        "clinical_trial_id":"coalesce(t.clinical_trial_id,'') As clinical_trial_id",
        "trial_arm":"a.arm_id+'_'+ a.arm_drug As trial_arm",
        "file_types":"COLLECT(DISTINCT(f.file_type)) AS file_types",
        "file_formats":"COLLECT(DISTINCT(f.file_format)) AS file_formats",
        "files":"COLLECT(DISTINCT(f)) AS files",
        "number_of_cases":"count(DISTINCT(c.case_id)) as number_of_cases",
    "number_of_trial":"count(DISTINCT(t.clinical_trial_designation)) as number_of_trial ",
    "number_of_files":"count(DISTINCT(f)) as number_of_files"
    },
    "condition1":{
      "clinical_trial_code":"t.clinical_trial_designation IN  @@@  ",
      "clinical_trial_id":"t.clinical_trial_id IN @@@",
      "pubmed_id":"a.pubmed_id  IN @@@",
      "arm_id":"a.arm_id  IN @@@",
      "arm_drug":"a.arm_drug  IN @@@",
      "disease":"c.disease IN @@@",
      "gender":"c.gender IN @@@",
      "race":"c.race IN @@@",
    },
    "condition2":{
       "data_type": "f.file_type IN @@@",
       "file_formats": "f.file_format IN @@@",
    }

}

# query builder
def QueryBuilder(input_filter,output_schema,base_query):
    # no input then given default query without conditions
    if bool(input_filter):
        return builder(input_filter, output_schema,base_query)
    else:
        return builder("",output_schema,base_query)

# combine the icdc_query
def builder(input_filters,output_schema,base_query)->str:
    query=[]
    query.append(base_query["part1"])

    # query add condition
    if input_filters!="":
        query.append("WHERE ")
        query.extend(builderWithCondition(input_filters,"condition1",base_query))

    # remove last AND if it presents
    if query[-1] == " AND" or query[-1] == "WHERE ":
        query.pop()

    query.append(str(base_query["part2"]))

    # query add file's condition
    if input_filters!="":
        query.append("WHERE ")
        query.extend(builderWithCondition(input_filters,"condition2",base_query))

    # remove last AND or Where if it presents
    if query[-1] == " AND" or query[-1] == "WHERE ":
        query.pop()

    query.append("RETURN ")
    #query add return
    query.extend(builderReturn(output_schema,base_query))
    if query[-1]==", ":
        query.pop()

    return  " ".join(query)

def builderWithCondition(input_filters,condition,base_query):
    output = []
    #base on the input filter to find the query then replace the place holder"@@@"" with real data
    for input_filter in input_filters:
        if input_filter in base_query[condition] and input_filters[input_filter]!="":
            output.append(base_query[condition][input_filter].replace("@@@",input_filters[input_filter]))
            output.append(" AND")

    return output

def builderReturn(output_schema,base_query):
    output=[]
    #base on the input filter to find the query and append to the query
    for schema in output_schema:
      if schema in base_query["return"]:
          output.append(base_query["return"][schema])
          output.append(", ")
    return output

# factory defines which query builder to use
def QueryBuilderFactory(input_type,input_schema,output_schema):

      query={
          "icdc": icdc_query,
          "ctdc": ctdc_query
      }
      return QueryBuilder(input_schema,output_schema,query[input_type])

app = Flask('app')

@app.route('/')
def main():
    icdc_filter = {
        "study_code": "",
        "study_type": "",
        "breed": "",
        "diagnosis": "",
        "disease_site": "",
        "stage_of_disease": "",
        "gender": "",
        "data_type": "",
        "file_formats": "",
    }

    icdc_output_schema = ["case_id", "study_code", "program", "study_type",
                   "breed", "diagnosis", "stage_of_disease", "disease_site", "age", "gender", "neutered_status",
                   "data_type", "file_formats", "files","number_of_files","number_of_sample","number_of_cases","number_of_study"]

    icdc_output_schema=["number_of_files","number_of_sample","number_of_cases","number_of_study"]

    ctdc_filter={
        "clinical_trial_code": "",
        "clinical_trial_id": "",
        "pubmed_id": "",
        "arm_id": "",
        "arm_drug": "",
        "disease": "",
        "gender": "",
        "race": "",
    }
    ctdc_output_schema=["case_id", "clinical_trial_code", "arm_id", "arm_drug", "pubmed_id", "disease", "gender", "race",
                   "ethnicity", "clinical_trial_id", "trial_arm", "file_types", "file_formats", "files","number_of_files","number_of_cases","number_of_trial"]
    ctdc_output_schema=["number_of_files","number_of_cases","number_of_trial"]

    return  " ".join(["icdc query: ",QueryBuilderFactory("icdc",icdc_filter,icdc_output_schema),"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ctdc query: ", QueryBuilderFactory("ctdc", ctdc_filter, ctdc_output_schema)])

app.run(host='0.0.0.0', port=8080)
