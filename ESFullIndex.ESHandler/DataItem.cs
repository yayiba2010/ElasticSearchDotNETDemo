using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ESFullIndex.ElasticSearchHandler
{
    public class DataItem
    {

         public int Id
        {
            get; set;
        }
        public String Body
        {
            get; set; 
        }

        public string Category
        {
            get; set;
        }
        public string Title
        {
            get; set;
        }

        public string KeyWordCodes
        {
            get; set;
        }

       public string KeyWords
        {
            get; set;
        }

         public DateTime PublishedTime
        {
            get; set;
        }

              public int categoryId
        {
            get; set;
        }
  
    }
}
