using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESFullIndex.ElasticSearchHandler;

using PlainElastic.Net;
using PlainElastic.Net.Queries;
using PlainElastic.Net.Serialization;

namespace ESFullIndex.Console
{
    class Program
    {
       public static ElasticConnection Client = new ElasticConnection("localhost", 9200);
        static void Main(string[] args)
        {
            
            

            //添加查询字符串索引
            SearchString();

            

            //添加查询文件索引

            SearchFileContent();

        }

        private static void SearchFileContent()
        {
            var data = GetFileData(AppDomain.CurrentDomain.BaseDirectory + @"files\ccflow.doc");

            var dataEncoding = Convert.ToBase64String(data); //转换为base64

            ElasticSearchHelper.Intance.Index("trying-out-mapper-attachments", "person", "1", new person2()
            {
                cv = dataEncoding
            });

            //查询文件内容
           
            string cmd = new SearchCommand("trying-out-mapper-attachments", "person");
            var query =
                new QueryBuilder<person2>().Query(
                    b =>
                        b.Bool(
                            m =>
                                m.Should(
                                    q =>
                                        q.QueryString(
                                            qs =>
                                                qs.Fields(new string[] {"cv"})
                                                    .Query("不支持低版本的IE浏览器")
                                                    .DefaultOperator(Operator.AND))))).Build();

            //文件内容查询结果
            var result = Client.Post(cmd, query);

            JsonNetSerializer serializer = new JsonNetSerializer();
            var list = serializer.ToSearchResult<person2>(result);
           
        }

        private static void SearchString()
        {
            //索引库
            string indexDB = "trying-string";
            //索引表
            string indexTable = "person";

            ElasticSearchHelper.Intance.Index(indexDB, indexTable, "1", new person2()
            {
                cv = @"单人玩游戏仅且有一个主人公（是个独眼龙的卡通模样）。
2.炸弹人样式游戏界面，敌人有食人花、狼人、吸血鬼、女巫、科学怪人、龙...每种敌人附属自己的巢穴如食人花盆、狼屋、吸血鬼棺材，巢穴的作用是在你消灭敌人后一段时间复制出敌人。方向键控制主人公躲闪敌人，道具散落在游戏界面中且在一定时间内会消失而每种道具仅对付一种敌人，如狼人用枪射杀、女巫要用魔法书；拾起的一个道具只能使用一只。对准敌人后ctrl键将拾起的道具使用攻击，杀死敌人后需要赶到正在复制生物的巢穴按Spacebar从天而降一条闪电炸毁，飞入下一回合，敌人数量种类将随之增加。
3.闯过一定层数才能打开下一个城堡。"
            });


            string cmd = new SearchCommand(indexDB, indexTable);
            


            //查询字符串
            //queryString 默认会做分词查询，为了达到全字匹配效果可以设置 .DefaultOperator(Operator.AND))) 所有查询的分词都要匹配;
            var query1 =
                new QueryBuilder<person2>().Query(
                    b =>
                                        b.QueryString(
                                            qs =>
                                                qs.Fields(new string[] {"cv"})
                                                    .Query("吸血鬼、女巫、科学怪人")
                                                    .DefaultOperator(Operator.AND))).Build();
            
           

            JsonNetSerializer serializer = new JsonNetSerializer();

            var result2 = Client.Post(cmd, query1);

            var list2 = serializer.ToSearchResult<person2>(result2);
        }
       
        /// <summary>
        /// 将文件转换成byte[] 数组
        /// </summary>
        /// <param name="fileUrl">文件路径文件名称</param>
        /// <returns>byte[]</returns>
        protected static byte[] GetFileData(string fileUrl)
        {
            FileStream fs = new FileStream(fileUrl, FileMode.Open, FileAccess.Read);
            try
            {
                byte[] buffur = new byte[fs.Length];
                fs.Read(buffur, 0, (int)fs.Length);

                return buffur;
            }
            catch (Exception ex)
            {
                //MessageBoxHelper.ShowPrompt(ex.Message);
                return null;
            }
            finally
            {
                if (fs != null)
                {

                    //关闭资源
                    fs.Close();
                }
            }
        }


        /// <summary>
        /// 将文件转换成byte[] 数组
        /// </summary>
        /// <param name="fileUrl">文件路径文件名称</param>
        /// <returns>byte[]</returns>

        protected static byte[] AuthGetFileData(string fileUrl)
        {
            using (FileStream fs = new FileStream(fileUrl, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                byte[] buffur = new byte[fs.Length];
                using (BinaryWriter bw = new BinaryWriter(fs))
                {
                    bw.Write(buffur);
                    bw.Close();
                }
                return buffur;
            }
        }
    }
}
