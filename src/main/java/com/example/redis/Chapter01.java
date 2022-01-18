package com.example.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static final void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        //connect本地的redis
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        String articleId = postArticle(
                conn, "wuhepeng", "A title", "http://www.google.com");

        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        //这里根据一篇文章的id得到文章的信息
        Map<String, String> articleData = conn.hgetAll("article:" + articleId);
        //遍历得到所有的信息
        for (Map.Entry<String, String> entry : articleData.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();
        //在一次运行中对一篇文章进行投票
        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        System.out.println("============================================================");
        System.out.println("The currently highest-scoring articles are:");
        List<Map<String, String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        addGroups(conn, articleId, new String[]{"new-group","new-group-one"});
        System.out.println("============================================================");
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    public String postArticle(Jedis conn, String user, String title, String link) {
        //计数器counter 执行incr命令来完成
        String articleId = String.valueOf(conn.incr("article:"));
        //投票文章的key
        String voted = "voted:" + articleId;
        //hash散列存储
        conn.sadd(voted, user);
        //设置过期的时间
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        long now = System.currentTimeMillis() / 1000;
        //文章
        String article = "article:" + articleId;
        //新建一个储存map直接用 hmset
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        //将文章信息储存在散列表内
        conn.hmset(article, articleData);
        //评分排序的有序集合
        conn.zadd("score:", now + VOTE_SCORE, article);
        //将文章添加到根据发布时间排序的有序集合
        conn.zadd("time:", now, article);

        return articleId;
    }

    public void articleVote(Jedis conn, String user, String article) {
        //cutoff是初次文章创建的时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;
        //小于这个时间证明是过期
        if (conn.zscore("time:", article) < cutoff) {
            return;
        }

        String articleId = article.substring(article.indexOf(':') + 1);
        //判断该文章的投票用户是否含有
        if (conn.sadd("voted:" + articleId, user) == 1) {
            //自增
            conn.zincrby("score:", VOTE_SCORE, article);
            //增加投票者
            conn.hincrBy(article, "votes", 1);
        }
    }

    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    public List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        //1 - 1 = 0
        int start = (page - 1) * ARTICLES_PER_PAGE;
        //25 每页显示25条数据
        int end = start + ARTICLES_PER_PAGE - 1;
        //排序从 大到小 集合 逆序排序
        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        //得到所有的id
        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        //生成article
        String article = "article:" + articleId;
        //遍历all group都加入一个article
        for (String group : toAdd) {
            //将文章增加到所属的群组
            conn.sadd("group:" + group, article);
        }

    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        //score:new-group
        String key = order + group;
        //检查缓存结果是否已缓存 排序的结果(zset),如果没有的话就现在进行排序(交集)
        if (!conn.exists(key)) {
            //根据评分或者发布时间对群组文章进行排序
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            //让redis在60秒之后自动删除这个有序集合
            conn.expire(key, 60);
        }

        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String, String>> articles) {

        for (Map<String, String> article : articles) {

            System.out.println("  id: " + article.get("id"));

            //把一个map变成 一对 key value 变成一个对象,然后装进set里 进行遍历
            for (Map.Entry<String, String> entry : article.entrySet()) {
                //上面已经打印出id 所以不再打印出来
                if (entry.getKey().equals("id")) {
                    continue;
                }

                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
