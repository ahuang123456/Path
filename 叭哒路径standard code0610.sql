原始表：anime_dwd.anime_dwd_dd_shuffle_hive_app_session
切割原理：以启动、点击行为、阅读及播放日志间时间间隔为切割基准，按日志先后顺序串联起来，相邻两条日志时间间隔超过2分钟，
则认为用户没有已经离开页面，没有进行任何操作，当前session结束

++++++++++++++++step 1: 检查数据
基线数据：de已经准备完成
各个业务线:检查de, stime, click, display投递情况
1. 可以看de在页面中分布pv是否合理
select count(distinct de) as de_cnt,rpage_id,rpage_name from 
anime_dwd.anime_dwd_dd_shuffle_hive_app_session
where dt='2019-05-29'
and type='final'
and log_type='click'
group by rpage_id,rpage_name

2. 可以适当进行抓包，看投递情况


++++++++++++++++step 2: 加入人群脸谱标签
通过原表中的device_id与脸谱中的key_id中链接；有一些业务线的设备号没有与脸谱打通，标签会有null值
一开始与原表相连，便于漏斗计算
create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_gender_step2 as
select de,rpage_name,rpage_id,platform_name,sex,sys_time
from
    (select de,iqy_device_id,rpage_name,rpage_id,platform_name,sys_time
    from anime_dwd.anime_dwd_dd_shuffle_hive_app_session
    where dt='2019-05-29' and type='final' and log_type='click'
    group by de,iqy_device_id,rpage_name,rpage_id,platform_name,sys_time) a
left join 
    (select key_id,sex
    from udw.device_mid_dd_snap_hive_qipu_face
    where dt='2019-05-29'
    group by key_id,sex) b
on a.iqy_device_id=b.key_id


++++++++++++++++step 3: 决定路径最小人数；将de的行为进行排序
step 1 中已经知道节点分布
1. 这里将页面分布情况按照de的分布情况展现，可以判断路径最小人数情况
2. 给每个de下的行为进行排序
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_de_allrpage_step3 as
select de,rowrank,rpage_name,rpage_id,platform_name,sex  
from  
(select de,rank() over(partition by de order by sys_time) as rowrank,rpage_name,rpage_id,platform_name,sex
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_gender_step2) t
group by de,rpage_name,rpage_id,rowrank,platform_name,sex 


++++++++++++++++step 4: 挑选起点与终点，终点不一定需要确定
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step4 as 
select a.de,a.rpage_name,a.rpage_id,a.rowrank,a.platform_name,a.sex
from longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_de_allrpage_step3 a
    inner join (
        select distinct de 
        from longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_de_allrpage_step3
        where rowrank=1 and rpage_id in ('commend')) b
on a.de=b.de
    inner join(        
        select de,min(rowrank) as play_rank
        from longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_de_allrpage_step3
        where rpage_id in ('readermg','player','reader_nov')
        group by de) t
on a.de=t.de     
where a.rowrank<=play_rank


++++++++++++++++step 5: 挑选重要页面，将无关的页面排除
在基线项目中，将一些页面事先去除
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step5 as 
select de,rpage_name,rpage_id,platform_name,rank() over(partition by de order by rowrank) as rowrank1,sex
from longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step4
where rpage_id in
    ('readermg',
    'commend',
    'bookshelf_collect',
    'unknown',
    'comicif_1',
    'reader_nov',
    'my',
    'player',
    'bookshelf_history',
    'home_new',
    'animationif',
    'cmprev',
    'community',
    'search_results',
    'search_all',
    'usercenter',
    'nov_if',
    'home') and rpage_name not in ('广告页','轻小说新人气榜','轻小说新畅销榜','轻小说新潜力榜');


++++++++++++++++step 6: 去除重复的页面 去重A->B->B    A->B
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step6 as 
select de,rpage_name,rpage_id,rowrank1,platform_name,sex 
from
(select a.*,
    case when a.rpage_id=b.rpage_id then '0'
    else a.rpage_id
    end as dengyu 
    from 
        (select * 
        from longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step5) a
    left join 
        (select * 
        from longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step5) b
        on a.de=b.de and a.rowrank1=1+b.rowrank1) t
where dengyu<>'0'



以上数据清理完成
++++++++++++++++step 7: 重新排序，因为计算上下游的时候，顺序是比较严格
create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7 as 
select de,rpage_name,rpage_id,rowrank2,platform_name,sex 
from 
(select de,rpage_name,rpage_id,platform_name,rank() over(partition by de order by rowrank1) as rowrank2
from 
longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_step6) t
group by de,rpage_name,rpage_id,rowrank2,platform_name,sex;


++++++++++++++++step 8: 重要页面占比 (可加人群)
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_fenbu_step8 as 
select count(distinct de) as de_cnt,rpage_name,rpage_id,rowrank2,platform_name,sex 
from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7
group by rpage_name,rpage_id,rowrank2,platform_name,sex;


++++++++++++++++step 9: 路径节点个数 (可加人群)
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_jiedian_step9 as 
select count(1) as de_cnt,max_num
from 
    (select max(rowrank2) as max_num,de
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7
    group by de) t
group by max_num


++++++++++++++++step 10: 路径上下游 (可加人群)
create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_around_step10 as 
select a.de,a.rpage_name,a.platform_name,a.rowrank2,b.rpage_name as rpage_pre,c.rpage_name as rpage_post
from 
    (select * 
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7) a 
left join 
    (select * 
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7) b
on a.de=b.de and a.rowrank2=b.rowrank2+1
left join 
    (select * 
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7) c
on a.de=c.de and a.rowrank2=c.rowrank2-1;

create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_around_step10_cnt as 
select rpage_name,rpage_pre,rpage_post,count(1) as cnt
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_around_step10
group by rpage_name,rpage_pre,rpage_post;



++++++++++++++++step 11: Top路径分析 (可加人群)
create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_part_path_step11 as  
select de,concat_ws('->',collect_list(rpage_name)) as path_no 
from 
(select de,rowrank2,rpage_name
    from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_again_step7
group by de,rowrank2,rpage_name
order by de,rowrank2,rpage_name) a
group by de

create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_part_path_step11_cnt as
select count(distinct de),path_no as de_cnt 
from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_part_path_step11
group by path_no


++++++++++++++++step 12: 路径分析人群
1. 将top5的de去重过滤出来
create table longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_part_path_hauxiang_step12 as  
select distinct de
from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_part_path_step11
where path_no in 
    ('动漫App-首页-推荐页130->动漫APP-阅读器',
    '动漫App-首页-推荐页130->动漫app-连续签到弹窗->动漫APP-阅读器',
    '动漫App-首页-推荐页130->漫画详情（新-v170）->动漫APP-阅读器',
    '动漫App-首页-推荐页130->动漫APP-书架-收藏->动漫APP-阅读器',
    '动漫App-首页-推荐页130->动漫App-轻小说-阅读器')

2. 将de的表拼回有device_id的原表
create table longyuan_ba.anime_dwd_dd_shuffle_hive_app_part_path_device_id as 
select iqy_device_id
from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_part_path_hauxiang_step12 a
left join 
    (select de,iqy_device_id
    from anime_dwd.anime_dwd_dd_shuffle_hive_app_session
    where dt='2019-05-29' and type='final' and log_type='click'
    group by de,iqy_device_id) b
on a.de=b.de 
group by iqy_device_id

3. 放入达芬奇看结果

4. 加入一些行为丰富用户特征,播放时长等


++++++++++++++++step 13: 漏斗分析，某些节点单独计算 (可加人群)
从拼上人群的排序好的表中进行计算：longyuan_ba.anime_dwd_dd_shuffle_hive_app_session_rank_de_allrpage_step3
例如de的第一步是首页
select count(distinct de) as de_cnt, sex
from longyuan_ba.hwh_anime_dwd_dd_shuffle_hive_app_session_rank_gender_book_all
where rowrank=1 and rpage_id in ('commend')
and sex is not null
group by sex 

++++++++++++++++step 14: 桑基图




