/**
 * Copyright (c) 2015-2016, Retire 吴益峰(wyf372310383@163.com)..
 * <p>
 * Licensed under the GNU Lesser General Public License (LGPL) ,Version 3.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.gnu.org/licenses/lgpl-3.0.txt
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jpress.searcher;

import com.jfinal.kit.PathKit;
import com.jfinal.kit.StrKit;
import com.jfinal.plugin.activerecord.Page;
import io.jpress.plugin.search.ISearcher;
import io.jpress.plugin.search.SearcherBean;
import io.jpress.utils.DateUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.lionsoul.jcseg.analyzer.v5x.JcsegAnalyzer5X;
import org.lionsoul.jcseg.tokenizer.core.JcsegTaskConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class LuceneSearcher implements ISearcher {
    private String mIndexFilePath;

    @Override
    public void init() {
        String luceneDiskStorePath = PathKit.getWebRootPath();
        File pathFile = new File(luceneDiskStorePath, ".lucene");
        if (!pathFile.exists()) pathFile.mkdirs();

        mIndexFilePath = pathFile.getPath();

        // 初始化
        try (IndexWriter indexWriter = createIndexWriter();) {

        } catch (IOException e) {
            throw new RuntimeException("LuceneSearcher init error!", e);
        }
    }


    @Override
    public void addBean(SearcherBean bean) {

        try {
            IndexWriter indexWriter = createIndexWriter();
            indexWriter.addDocument(createDocument(bean));
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void deleteBean(String beanId) {
        try {
            IndexWriter indexWriter = createIndexWriter();
            Term term = new Term("sid", beanId);
            indexWriter.deleteDocuments(term);
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void updateBean(SearcherBean bean) {
        try {
            IndexWriter indexWriter = createIndexWriter();
            Term term = new Term("sid", bean.getSid());
            indexWriter.updateDocument(term, createDocument(bean));
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Page<SearcherBean> search(String keyword, String module) {
        return search(keyword, module, 1, 20);
    }

    @Override
    public Page<SearcherBean> search(String queryString, String module, int pageNum, int pageSize) {
        List<SearcherBean> list = new ArrayList<SearcherBean>();
        try {
            IndexSearcher mIndexSearcher = getIndexSearcher();
            queryString = QueryParser.escape(queryString);

            String[] queries = {queryString, queryString, queryString};
            String[] fields = {"title", "description", "content"};
            BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD, BooleanClause.Occur.SHOULD};
            Query query = MultiFieldQueryParser.parse(queries, fields, flags, new JcsegAnalyzer5X(JcsegTaskConfig.COMPLEX_MODE));
            TopDocs topDocs = mIndexSearcher.search(query, 1000);//1000,最多搜索1000条结果

            if (topDocs != null && topDocs.totalHits > 0) {
                ScoreDoc[] scoreDocs = topDocs.scoreDocs;
                for (int i = 0; i < scoreDocs.length; i++) {
                    int docId = scoreDocs[i].doc;
                    Document doc = mIndexSearcher.doc(docId);
                    list.add(createSearcherBean(doc));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Page<SearcherBean>(list, pageNum, pageSize, list.size() / pageSize,
                list.size());
    }

    public IndexWriter createIndexWriter() throws IOException {
        if (mIndexFilePath == null) {
            throw new NullPointerException("please invoke init() method first!");
        }

        Analyzer analyzer = new JcsegAnalyzer5X(JcsegTaskConfig.COMPLEX_MODE);

        // 非必须(用于修改默认配置): 获取分词任务配置实例
        JcsegAnalyzer5X jcseg = (JcsegAnalyzer5X) analyzer;
        // 追加同义词到分词结果中, 需要在jcseg.properties中配置jcseg.loadsyn=1
        JcsegTaskConfig config = jcseg.getTaskConfig();
        // 追加拼音到分词结果中, 需要在jcseg.properties中配置jcseg.loadpinyin=1
        config.setAppendCJKSyn(true);
        // 更多配置, 请查看com.webssky.jcseg.core.JcsegTaskConfig类
        config.setAppendCJKPinyin(true);

        Directory fsDirectory = FSDirectory.open(Paths.get(mIndexFilePath));
        IndexWriterConfig indexConfig = new IndexWriterConfig(analyzer);
        indexConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        indexConfig.setMaxBufferedDocs(1000);
        IndexWriter indexWriter = new IndexWriter(fsDirectory, indexConfig);
        return indexWriter;
    }

    /**
     * 获取IndexReader
     *
     * @return
     */
    public IndexSearcher getIndexSearcher() {
        try {
            Directory directory = FSDirectory.open(Paths.get(mIndexFilePath));
            IndexReader ireader = DirectoryReader.open(directory);
            return new IndexSearcher(ireader);
        } catch (IOException e) {
            throw new RuntimeException("getIndexSearcher error!", e);
        }
    }

    /**
     * 创建文档，用于生成索引
     *
     * @param bean
     * @return
     */
    private Document createDocument(SearcherBean bean) {
        Document document = new Document();
        document.add(new TextField("content", bean.getContent(), Field.Store.YES));
        document.add(new TextField("title", bean.getTitle(), Field.Store.YES));
        document.add(new StringField("url", bean.getUrl(), Field.Store.YES));
        document.add(new TextField("description", bean.getDescription(), Field.Store.YES));
        document.add(new StringField("sid", bean.getSid(), Field.Store.YES));
        document.add(new StringField("created", DateUtils.format(bean.getCreated()), Field.Store.YES));
        return document;
    }

    /**
     * 查询结果转换
     *
     * @param doc
     * @return
     */
    private SearcherBean createSearcherBean(Document doc) {
        SearcherBean searcherBean = new SearcherBean();
        String content = doc.get("content");
        String title = doc.get("title");
        String description = doc.get("description");
        String sid = doc.get("sid");
        String url = doc.get("url");
        String created = doc.get("created");
        if (StrKit.notBlank(created)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                searcherBean.setCreated(sdf.parse(created));
            } catch (java.text.ParseException e) {
                e.printStackTrace();
            }
        }
        searcherBean.setContent(content);
        searcherBean.setTitle(title);
        searcherBean.setDescription(description);
        searcherBean.setSid(sid);
        searcherBean.setUrl(url);
        return searcherBean;
    }

}
