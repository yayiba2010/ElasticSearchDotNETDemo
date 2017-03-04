using Newtonsoft.Json;
using PlainElastic.Net;
using PlainElastic.Net.Mappings;
using PlainElastic.Net.Queries;
using PlainElastic.Net.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ESFullIndex.ElasticSearchHandler
{
    /// <summary>
    /// ik分词结果对象
    /// </summary>
    public class ik
    {
        public List<tokens> tokens { get; set; }
    }
    public class tokens
    {
        public string token { get; set; }
        public int start_offset { get; set; }
        public int end_offset { get; set; }
        public string type { get; set; }
        public int position { get; set; }
    }

    /// <summary>
    /// 测试数据对象
    /// </summary>
    public class personList
    {
        public personList()
        {
            this.list = new List<person>();
        }
        public int hits { get; set; }
        public int took { get; set; }
        public List<person> list { get; set; }
    }
    public class person
    {
        public string id { get; set; }
        public string name { get; set; }
        public int age { get; set; }
        public bool sex { get; set; }
        public DateTime birthday { get; set; }
        public string intro { get; set; }
    }

    public class person2
    {
        public string cv { get; set; }
     
    }

    public class jdbc
    {

        public string Id { get; set; }
        public int? InfoId { get; set; }
        //public int? CategoryId { get; set; }
        public string KeyWordCodes { get; set; }
        public string KeyWords { get; set; }
        public string Title { get; set; }
        public string Content { get; set; }
        public DateTime PublishedTime { get; set; }
        //public string CategoryTitle { get; set; }


    }
    public class ElasticSearchHelper
    {
        public static readonly ElasticSearchHelper Intance = new ElasticSearchHelper();
        private ElasticConnection Client;
        private ElasticSearchHelper()
        {

            var node = new Uri("http://localhost:9200");

            Client = new ElasticConnection("localhost", 9200);



        }
        /// <summary>
        /// 数据索引
        /// </summary>       
        /// <param name="indexName">索引名称</param>
        /// <param name="indexType">索引类型</param>
        /// <param name="id">索引文档id，不能重复,如果重复则覆盖原先的</param>
        /// <param name="jsonDocument">要索引的文档,json格式</param>
        /// <returns></returns>
        public IndexResult Index(string indexName, string indexType, string id, string jsonDocument)
        {

            var serializer = new JsonNetSerializer();
            string cmd = new IndexCommand(indexName, indexType, id);
            OperationResult result = Client.Put(cmd, jsonDocument);

            var indexResult = serializer.ToIndexResult(result.Result);
            return indexResult;
        }
        public IndexResult Index(string indexName, string indexType, string id, object document)
        {
            var serializer = new JsonNetSerializer();
            var jsonDocument = serializer.Serialize(document);
            return Index(indexName, indexType, id, jsonDocument);
        }

        /// <summary>
        /// 关键字查询条件
        /// </summary>
        /// <param name="keyWordsCodes"></param>
        /// <returns></returns>
        public QueryString<jdbc> KeyWordCodesQuery(string[] keyWordsCodes)
        {
            var charStr = "\"";
            var query = new QueryString<jdbc>();
            query = query.Fields("KeyWordCodes");
            foreach (var code in keyWordsCodes)
            {
                query = query.Query(charStr + code + charStr);
            }
            return query;
        }



        public BoolQuery<jdbc> GetQuery(string key, string[] codes)
        {
            var chart = "\"";
            BoolQuery<jdbc> query = new BoolQuery<jdbc>();
            query = query.Must(t => t.QueryString(t1 => t1.Fields(new string[] { "Title", "Content" }).Query(key)));
            BoolQuery<jdbc> codeQuery = new BoolQuery<jdbc>();
            //query.Must(t => t.Bool(
            //    e => e.Should(e2 => e2.QueryString(e1 => e1.Fields("KeyWordCodes").Query("\"BQK_XWZX\"")))
            //    .Should(e5 => e5.QueryString(e3 => e3.Fields("KeyWordCodes").Query("\"BQK_XWZX_2\"")))));
            //query.Must(t => t.QueryString(t1 => t1.Fields("KeyWordCodes").Query("[\"BQK_XWZX\",\"BQK_XWZX_2\"]")));
            foreach (var item in codes)
            {
                codeQuery = codeQuery.Should(e => e.QueryString(e1 => e1.Fields("KeyWordCodes").Query(chart + item + chart).DefaultOperator(Operator.AND)));
            }
            query.Must(t => t.Bool(
           e => codeQuery));
            return query;
        }

        public SearchResult<jdbc> QueryByKeyWordCodes(string indexName, string indexType, string key, string[] keyWordsCodes, int pageIndex,
         int pageSize)
        {
            int from = (pageIndex - 1) * pageSize;
            var charStr = "\"";
            key = charStr + key + charStr;
            string cmd = new SearchCommand(indexName, indexType);
            string query = new QueryBuilder<jdbc>().Query(b => b.Bool(m => GetQuery(key, keyWordsCodes)))

                //分页
                .From(from)
                .Size(pageSize).Sort(e => e.Field("PublishedTime", SortDirection.desc))
                 //排序
                 // .Sort(c => c.Field("age", SortDirection.desc))
                 //添加高亮
                 .Highlight(h => h
                     .PreTags("<b>")
                     .PostTags("</b>")
                     .Fields(
                            f => f.FieldName("Content").Order(HighlightOrder.score),
                            f => f.FieldName("_all")
                            ).FragmentSize(150)
                    )
                   .Build();


            var result = Client.Post(cmd, query);

            var serializer = new JsonNetSerializer();
            var list = serializer.ToSearchResult<jdbc>(result);
            return list;

        }

        public SearchResult<jdbc> Search2<jdbc>(string indexName, string indexType, string key, string[] keyWordsCodes, int from, int size)
        {
            var charStr = "\"";
            key = charStr + key + charStr;
            string cmd = new SearchCommand(indexName, indexType);



            string query = new QueryBuilder<ElasticSearchHandler.jdbc>().Query(b => b.Bool(m => GetQuery(key, keyWordsCodes)))

              //分页
              .From(from)
              .Size(size).Sort(e => e.Field("PublishedTime", SortDirection.desc))
               //排序
               // .Sort(c => c.Field("age", SortDirection.desc))
               //添加高亮
               .Highlight(h => h
                   .PreTags("<b>")
                   .PostTags("</b>")
                   .Fields(
                          f => f.FieldName("Content").Order(HighlightOrder.score),
                          f => f.FieldName("_all")
                          ).FragmentSize(150)
                  )
                 .Build();


            var result = Client.Post(cmd, query);

            var serializer = new JsonNetSerializer();
            SearchResult<jdbc> list = null;

            list = serializer.ToSearchResult<jdbc>(result);

      


            return list;

        }




        //全文检索，多字段 并关系
        //搜索age在100到200之间，并且字段intro 或者name 包含词组key

        //将语句用ik分词，返回分词结果的集合
        private List<string> GetIKTokenFromStr(string key)
        {
            string s = "/db_test/_analyze?analyzer=ik";
            var result = Client.Post(s, "{" + key + "}");
            var serializer = new JsonNetSerializer();
            var list = serializer.Deserialize(result, typeof(ik)) as ik;
            return list.tokens.Select(c => c.token).ToList();
        }


    }
}
