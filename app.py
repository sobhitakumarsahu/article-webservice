import datetime

import boto3
import time
import json
import csv
import io
from datetime import datetime

def respond(statusCode, err, res=None):
    return {
        'statusCode': statusCode,
        'body': json.dumps(err) if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }

def getData(athena_client,session, site, requiredTags, tags, tagsExcept, category, categoryExcept, limit, subCategory, formattedStartdate, formattedEndDate,formattedViewStartdate, formattedViewEnddate, subCategoryExcept):
    #queryStrUrl = "replace(url,'https://" + site + "','') as url";
    queryStrUrl = "replace(regexp_replace(url, 'https?://', ''), '" + site + "', '') as url";
    RESULT_OUTPUT_LOCATION = "s3://smg-datalake-prod-athena-query-results/python-athena/";
    requiredTagsArr = requiredTags.split(',');
    requiredTagsArrStr = str(','.join(repr(str(i)) for i in requiredTagsArr));
    tagsArr = tags.split(',');
    tagsArrStr = str(','.join(repr(str(i)) for i in tagsArr));
    categoryArr = category.split(',');
    categoryArrStr = ','.join(repr(str(i)) for i in categoryArr);

    subCategoryArr = subCategory.split(',');
    subCategoryArrStr = ','.join(repr(str(i)) for i in subCategoryArr);

    print(requiredTagsArrStr);
    print(tagsArrStr);
    print(categoryArrStr);
    strVar = str(categoryArrStr);
    strVarSubcategory = str(subCategoryArrStr);
    subSectionMatchField = "";
    subSectionMatchFilter = "";
    print(limit);
    if category != "":
        print("sectionMatchField");
        sectionMatchField = ",cardinality(array_intersect(regexp_split(regexp_replace(trim(lower(section)), '\s+', ' '), '\s*(,|&)\s*'),"\
                       + " ARRAY[" + strVar + "])) AS section_match";

        print(sectionMatchField);

        if subCategory != "":
            print("subSectionMatchField");
            subSectionMatchField = ",cardinality(array_intersect(regexp_split(regexp_replace(trim(lower(subsection)), '\s+', ' '), '\s*(,|&)\s*')," \
                                + " ARRAY[" + strVarSubcategory + "])) AS sub_section_match";
            print("subSectionMatchField");
        else:
            subSectionMatchField = "";
    else:
        sectionMatchField = "";

    if sectionMatchField != "":
        sectionMatchFilter = "AND (section_match > 0)";

        if subSectionMatchField != "":
            subSectionMatchFilter = "AND (sub_section_match > 0)";
        else:
            subSectionMatchFilter = "";
    else:
        sectionMatchFilter = "";

    if tags != "":
        print("tagMatchField");
        tagMatchField = ",cardinality(array_intersect(" + " regexp_split(regexp_replace(trim(lower(concat(tags, ',', coalesce(invisible_tags, '')))), '\s+', ' '), '\s*,\s*')," \
                   + " ARRAY[" + tagsArrStr + "])) AS tag_match";

        print(tagMatchField);
    else:
        tagMatchField = "";

    if tagMatchField != "":
        tagMatchFilter = "AND (tag_match > 0)";
    else:
        tagMatchFilter = "";

    sortMetric = "";
    table = "summitmedia_pluma_api.summitmedia__article__top_articles_dup";
    domain = site;
    if requiredTags != "":
        print("requiredTagsFilter");
        requiredTagsFilter = "AND (cardinality(array_intersect(" \
                   + " regexp_split(regexp_replace(trim(lower(concat(tags, ',', coalesce(invisible_tags, '')))), '\s+', ' '), '\s*,\s*')," \
                   + " ARRAY[" + requiredTagsArrStr + "])) = cardinality(array_distinct(ARRAY[" + requiredTagsArrStr + "])))";

        print(requiredTagsFilter);
    else:
        requiredTagsFilter = "";
    tagsExceptFilter = "";
    categoryExceptFilter = "";
    subCategoryExceptFilter = "";
    if tagsExcept != "":
        tagsExceptArr = tagsExcept.split(',');
        tagsExceptArrStr = str(','.join(repr(str(i)) for i in tagsExceptArr));
        tagMatchField = "";
        tagMatchFilter = "";
        requiredTagsFilter = "";
        tagsExceptFilter = "AND (cardinality(array_intersect(" + " regexp_split(regexp_replace(trim(lower(concat(tags, ',', coalesce(invisible_tags, '')))), '\s+', ' '), '\s*,\s*')," \
                   + " ARRAY[" + tagsExceptArrStr + "]))=0)";

    if category != "":
        if subCategoryExcept != "":
            subCategoryExceptArr = subCategoryExcept.split(',');
            subCategoryExceptArrStr = str(','.join(repr(str(i)) for i in subCategoryExceptArr));
            subSectionMatchField = "";
            subSectionMatchFilter = "";
            requiredTagsFilter = "";
            subCategoryExceptFilter = "AND (cardinality(array_intersect(regexp_split(regexp_replace(trim(lower(subsection)), '\s+', ' '), '\s*(,|&)\s*')," \
                                      + " ARRAY[" + subCategoryExceptArrStr + "])) = 0)";

    if categoryExcept != "":
        categoryExceptArr = categoryExcept.split(',');
        categoryExceptArrStr = str(','.join(repr(str(i)) for i in categoryExceptArr));
        sectionMatchField = "";
        sectionMatchFilter = "";
        subSectionMatchField = "";
        subSectionMatchFilter = "";
        requiredTagsFilter = "";
        subCategoryExceptFilter = "";
        categoryExceptFilter = "AND (cardinality(array_intersect(regexp_split(regexp_replace(trim(lower(section)), '\s+', ' '), '\s*(,|&)\s*'),"\
                       + " ARRAY[" + categoryExceptArrStr + "])) = 0)";



    partitionSortOrder = "";
    if sectionMatchField != "":
        partitionSortOrder = partitionSortOrder + ", section_match DESC";
    if subSectionMatchField != "":
        partitionSortOrder = partitionSortOrder + ", sub_section_match DESC";
    if tagMatchField != "":
        partitionSortOrder = partitionSortOrder + ", tag_match DESC";


    query = f"""
    WITH level1 AS (
	   SELECT 
	      {queryStrUrl}
	      ,section as category
	     ,published_date
	     ,author
	     ,title
	     ,max(pageviews) AS pageview
	     ,domain
	     ,slug
	     ,article_id
	     ,subsection	    
	     ,tags
	     ,invisible_tags
	     {sectionMatchField}
	     {subSectionMatchField}
	     {tagMatchField}	     
	     FROM {table}
	   WHERE (published_date BETWEEN DATE '{formattedStartdate}' and DATE '{formattedEndDate}')
	   and (viewdate BETWEEN DATE '{formattedViewStartdate}' and DATE '{formattedViewEnddate}')
	   AND (domain IN ('{domain}'))
	   {requiredTagsFilter}
	   {tagsExceptFilter}
	   {categoryExceptFilter}
	   {subCategoryExceptFilter}
	   group by url,section,published_date
             ,author
             ,title
             ,domain
             ,slug
             ,article_id
             ,subsection            
             ,tags
             ,invisible_tags	   
	), data AS (
	   SELECT
	      *
	     ,row_number() OVER (PARTITION BY domain ORDER BY pageview DESC {partitionSortOrder} , published_date DESC) index
	   FROM level1
	   WHERE true
	   {sectionMatchFilter}
	   {subSectionMatchFilter}
	   {tagMatchFilter}
	)
	SELECT
	   *
	FROM data
	WHERE true
	AND (index <= {limit})
	ORDER BY domain, index
    """;

    print ("Query : " + query);
    database = 'summitmedia_pluma_api';
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION}
    );
    queryId = response['QueryExecutionId'];
    key = f'{queryId}' + ".csv";
    time.sleep(10);
    s3 = session.client('s3')
    bucket_name = 'smg-datalake-prod-athena-query-results';
    subdirectory_path = 'python-athena/';
    s3_file_path = subdirectory_path + key;
    obj = s3.get_object(Bucket=bucket_name, Key=s3_file_path);
    file_contents = obj['Body'].read().decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(file_contents))
    json_data = json.dumps(list(csv_reader))
    return json_data;


def lambda_handler(event, context):

    AWS_ACCESS_KEY = "AKIAVSBZUZPCZJ3HCNNM"
    AWS_SECRET_KEY = "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"
    AWS_REGION = "ap-southeast-1"

    athena_client = boto3.client(
            "athena",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
        );
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    );
    args = event["queryStringParameters"]
    if 'site' in args:
        site = args['site'].lower();

        if 'required-tags' in args:
            requiredTags = args['required-tags'];
        else:
            requiredTags = '';

        if 'tags' in args:
            tags = args['tags'];
        else:
            tags = '';

        if 'tags-except' in args:
            tagsExcept = args['tags-except'];
        else:
            tagsExcept = '';

        if 'category' in args:
            category = args['category'];
        else:
            category = '';

        if 'category-except' in args:
            categoryExcept = args['category-except'];
        else:
            categoryExcept = '';

        if 'sub-category-except' in args:
            subCategoryExcept = args['sub-category-except'];
        else:
            subCategoryExcept = '';

        if 'limit' in args:
            limit = args['limit'];
        else:
            limit = 10;

        if 'sub-category' in args:
            subCategory = args['sub-category'];
        else:
            subCategory = '';

        if 'pub-date' in args:
            dateRange = args['pub-date'];
            startDateStr, endDateStr = dateRange.split("-")
            startDate = datetime.strptime(startDateStr, "%Y%m%d")
            endDate = datetime.strptime(endDateStr, "%Y%m%d")
            formattedStartdate = startDate.strftime("%Y-%m-%d")
            formattedEndDate = endDate.strftime("%Y-%m-%d")

        else:
            date_str = "1990-01-01";
            date_obj = datetime.strptime(date_str, "%Y-%m-%d");
            formattedStartdate = date_obj.strftime("%Y-%m-%d");
            current_date = datetime.today().strftime('%Y-%m-%d');
            formattedEndDate = current_date;

        if 'view-date' in args:
            dateRange = args['view-date'];
            startDateStr, endDateStr = dateRange.split("-")
            startDate = datetime.strptime(startDateStr, "%Y%m%d")
            endDate = datetime.strptime(endDateStr, "%Y%m%d")
            formattedViewStartdate = startDate.strftime("%Y-%m-%d")
            formattedViewEndDate = endDate.strftime("%Y-%m-%d")

        else:
            date_str = "1990-01-01";
            date_obj = datetime.strptime(date_str, "%Y-%m-%d");
            formattedViewStartdate = date_obj.strftime("%Y-%m-%d");
            current_date = datetime.today().strftime('%Y-%m-%d');
            formattedViewEndDate = current_date;

    else:
        errorMsg = {'message': 'Missing parameter. site, requiredTags, tags and limit are required',
                    'result': [],
                    'status': 'ERROR'}
        statusCode = 400
        return respond(statusCode, errorMsg);
    try:
        json_data = getData(athena_client, session, site.lower(), requiredTags.lower(), tags.lower(), tagsExcept.lower(), category.lower(), categoryExcept.lower(), limit, subCategory.lower(), formattedStartdate, formattedEndDate, formattedViewStartdate, formattedViewEndDate, subCategoryExcept);
    # Return JSON response
        return {
                'statusCode': 200,
                'body': json_data,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
    except Exception as err:
        print("Error: " + str(err))
        errorMsg = {'site': args['site'],
                    'message': 'Unable to fetch articles with given parameters',
                    'result': [],
                    'requiredTags': args['required-tags'],
                    'tags': args['tags'],
                    'limit': args['limit'],
                    'status': 'ERROR'}
        return respond(400, errorMsg, None)

