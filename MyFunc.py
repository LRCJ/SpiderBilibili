# -*- coding: utf-8 -*-
from urllib import request, error, parse
import os, time, json, mysql.connector,logging

########## 配置日志记录 ##########
logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG,filename="log.txt",filemode="a")

########## 发出http请求 ##########
def GetPage(Url, Data={}, Headers={}, Method=0):
    try:
        if Method == 0:  # get请求
            Data = parse.urlencode(Data)
            req = request.Request(url='%s%s%s' % (Url, '?', Data), headers=Headers)
        elif Method ==1:  # post请求（content-type为其它，如form-data等）
            Data = (parse.urlencode(Data)).encode('utf-8')
            req = request.Request(Url, data=Data, headers=Headers)
        elif Method == 2: # Post请求（content-type为application/json）
            Data = json.dumps(Data).encode('utf-8')
            req = request.Request(Url, data=Data, headers=Headers)
        else:
            pass
        Resp = request.urlopen(req)  # 发出请求
    except error.URLError as e:
        logging.error("http请求过程发生错误！%s"%e)
        return 0
    return Resp

########## 打印进度条 ##########
# def ProgressBar(precent,length=50,end_str=""):
#     count = round(precent*length)
#     progress_bar = "%4.1f"%(precent*100)+'%'+'['+'='*count+'>'+'-'*(length-count)+']'+end_str;
#     print('\r'+progress_bar,end='',flush=True)

########## 将数据存入数据库 ##########
def StoreDB(*cmd,Host="localhost",User="liaoren",
            Passwd="x8hJxaIbQ",Auth="mysql_native_password",
            DB="bilibili_data"):
    con = mysql.connector.connect(host=Host,user=User,password=Passwd,auth_plugin=Auth,database=DB)
    curson = con.cursor()
    try:
        for i in cmd:
            curson.execute(i)
            con.commit()            #对数据库进行增删改必须要使用该函数
    except BaseException as e:
        logging.error("执行数据库存储命令发生错误！%s"%e)
        curson.close()
        con.close()
        return 0
    curson.close()
    con.close()
    return 1

########## 查询数据库数据 ##########
def QueryDB(cmd,Host="localhost",User="liaoren",
            Passwd="x8hJxaIbQ",Auth="mysql_native_password",
            DB="bilibili_data"):
    con = mysql.connector.connect(host=Host,user=User,password=Passwd,auth_plugin=Auth,database=DB)
    curson = con.cursor(buffered=True)
    try:
        curson.execute(cmd)
        result = curson.fetchall()
    except BaseException as e:
        logging.error("执行数据库查询命令发生错误！%s"%e)
        curson.close()
        con.close()
        return 0
    curson.close()
    con.close()
    return result


########## 获取指定UP主的个人信息 ##########
def GetUPinfo(Store=False):
    ##### 变量
    table = "up_info"
    ##### 判断是否要删除无需再爬取数据的UP的数据
    result = QueryDB("SELECT uid FROM %s WHERE NOT EXISTS (SELECT uid FROM uid_list WHERE uid_list.uid=%s.uid);"%(table,table))
    if len(result) != 0:
        logging.info("准备删除无需再爬取的UP基本数据！")
        cmd = []
        for i in result:
            cmd.append("DELETE FROM %s WHERE uid = '%s';" % (table,i[0]))
        result = StoreDB(*cmd)
        if result == 1:
            logging.info("成功删除无需再查询信息的UP基本数据！")
        else:
            logging.info("删除无需再查询信息的UP基本数据失败！")
            return 0
    ##### 判断是否要更新UP数据
    result = QueryDB("SELECT uid FROM uid_list WHERE NOT EXISTS (SELECT uid FROM %s WHERE %s.uid=uid_list.uid);"%(table,table))
    if len(result) == 0:#根据数据库中的uid_list表来确定需不需要查询新的up主的信息或删除up信息
        logging.info("无需更新UP基本数据！")
    else:
        logging.info("开始爬取UP基本数据！")
        up_info = []
        try:
            for i in result:
                url = "https://api.bilibili.com/x/space/acc/info"            #获取指定up的个人信息
                para = {
                    "mid": i[0],
                    "jsonp": "jsonp"
                }
                header = {
                    "User-Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.122Safari / 537.36"
                }
                resp = GetPage(url,Data=para,Headers=header)
                data = json.load(resp)
                up_info.append({
                    "uid":data['data']['mid'],                          #uid号
                    "name":data['data']['name'],                        #昵称
                    "sex":data['data']['sex'],                          #性别
                    "level":data['data']['level'],                      #等级
                    "birthday":data['data']['birthday'],                #生日
                    "official":data['data']['official']['title'],       #认证称号
                    "official_type":data['data']['official']['type'],   #认证类型
                    "face":data['data']['face']                         #头像
                })
        except BaseException as e:
            logging.error("爬取UP基本数据出错，请检查爬虫响应数据！%s"%e)
            return 0
        if Store:       #是否要存储到数据库中
            cmd = ["INSERT INTO %s(uid,name,sex,level,birthday,official,official_type,face) " \
                      "SELECT %s,'%s','%s',%s,'%s','%s',%s,'%s' FROM dual " \
                      "WHERE NOT EXISTS (SELECT %s FROM %s WHERE uid = %s);"%(
                    table,
                    i["uid"],i["name"],i["sex"],i["level"],
                    i["birthday"],i["official"],i["official_type"],i["face"],
                    i["uid"],table,i["uid"]
                ) for i in up_info]
            result = StoreDB(*cmd)
            if result == 1:
                logging.info("存储UP基本数据成功！")
                return 1
            else:
                logging.info("存储UP基本数据失败！")
                return 0
        else:
            return up_info


########## 获取指定UP主的粉丝数 ##########
def GetFans(Store=False):
    ##### 变量
    table = "fans"
    ##### 删除无需再爬取数据的UP的粉丝数据
    result = StoreDB("DELETE FROM %s WHERE uid NOT IN (SELECT uid FROM uid_list);"%table)
    if result == 1:
        logging.info("没有需要删除/成功删除无需再查询信息的UP粉丝数据！")
    else:
        logging.info("删除无需再查询信息的UP粉丝数据失败！")
    #### 查询uid_list表，获取需要查询数据的UP的uid
    result = QueryDB("SELECT uid FROM uid_list;")
    ##### 根据uid号爬取粉丝数据
    fans = []
    for i in result:
        url = "https://api.bilibili.com/x/relation/stat"       #获取指定up的粉丝数
        para = {
            "vmid":i[0],
            "jsonp":"jsonp",
            "callback":"__jp3"
        }
        header = {
            "User-Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.122Safari / 537.36",
            "Referer":"https://space.bilibili.com/"+str(i[0])
        }
        resp = GetPage(url,Data=para,Headers=header)
        try:
            data  = (resp.read()).decode("utf-8")
            data = json.loads(data[6:-1])
            fans.append({
                "uid":data['data']['mid'],          #uid号
                "fans":data['data']['follower'],    #粉丝数
                "time":round(time.mktime(time.strptime((resp.getheaders())[0][1],"%a, %d %b %Y %H:%M:%S GMT")) + 3600 * 8)           #时间
            })
        except BaseException as e:
            logging.error("爬取UP粉丝数据出错，请检查爬虫响应数据！")
            return 0
    ##### 将粉丝数据存入数据库
    if Store:
        cmd = "INSERT INTO %s(uid,fans,time) VALUES "%table
        for i in fans:
            cmd = cmd + "(%d,%d,%d),"%(i["uid"],i["fans"],i["time"])
        result = StoreDB(cmd[:-1]+";")
        if result == 1:
            logging.info("存储UP粉丝数据成功！")
            return 1
        else:
            logging.info("存储UP粉丝数据失败！")
            return 0
    else:
        return fans


########## 获取指定UP主发布的所有视频的bvid及基本信息 ##########
def GetVideoList(Uid,Store=False):
    ##### 变量
    table = "videos_list"
    ##### 爬取视频列表数据
    url = "https://api.bilibili.com/x/space/arc/search"   #获取该up投稿的所有视频的bvid号
    para = {
        "mid": Uid,
        "ps": 30,
        "tid": 0,
        "pn": 0,            #页数
        "keyword":"",
        "order": "pubdate",
        "jsonp": "jsonp"
    }
    header = {
        "User-Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.122Safari / 537.36"
    }
    videos_list = []
    try:
        while True:
            para["pn"] = para["pn"] + 1
            resp = GetPage(url,Data=para,Headers=header)
            time.sleep(1)
            data = json.load(resp)
            for i in data["data"]["list"]["vlist"]:
                videos_list.append({
                    "uid":i["mid"],         #视频作者uid号
                    "bvid":i["bvid"],       #视频bv号
                    "aid":i["aid"],         #视频av号
                    "title":(i["title"].replace("\'","\\'")).replace("\"",'\\"'),   #视频标题
                    "pic":i["pic"],         #视频封面
                    "ctime":i["created"],   #投稿时间
                    "length":i["length"]    #视频时长
                })
            # ProgressBar(len(videos_list)/data["data"]["page"]["count"],end_str="总视频数为%d！"%data["data"]["page"]["count"])
            if len(videos_list) == data["data"]["page"]["count"]:
                # print("\n")
                break
    except BaseException as e:
        logging.error("爬取视频列表数据出错，请检查爬虫响应数据！%s"%e)
        return 0
    ##### 将视频列表数据存入数据库
    if Store:
        cmd = ["INSERT INTO %s(uid,bvid,aid,title,pic,ctime,length) SELECT %s,'%s',%s,'%s','%s',%s,'%s' " \
              "FROM dual WHERE NOT EXISTS (SELECT '%s' FROM %s WHERE bvid = '%s');"%(
            table,
            i["uid"],i["bvid"],i["aid"],i["title"],i["pic"],i["ctime"],i["length"],
            i["bvid"],table,i["bvid"]) for i in videos_list] ##### 需要保证在不报错的情况下不重复插入
        result = StoreDB(*cmd)
        if result == 1:
            logging.info("存储视频列表数据成功！")
            return 1
        else:
            logging.info("存储视频列表数据失败！")
            return 0
    else:
        return videos_list
########## 根据uid_list中的uid号查询更新数据 ##########
def GetVideoListNew():
    ##### 变量
    table = "videos_list"
    ##### 判断是否要删除无需再爬取数据的UP的数据
    # result = QueryDB("SELECT uid FROM %s WHERE NOT EXISTS (SELECT uid FROM uid_list WHERE uid_list.uid=%s.uid) GROUP BY uid;"%(table,table))
    # if len(result) != 0:
    #     logging.info("准备删除视频列表中无需再爬取的数据！")
    #     print("%s:"%time.ctime())
    #     cmd = ["DELETE FROM %s WHERE uid = '%s';" %(table,i[0]) for i in result]
    #     result = StoreDB(*cmd, DB="newdb")
    #     if result == 1:
    #         print("%s:成功删除无需再查询信息的视频列表数据！"%time.ctime())
    #     else:
    #         print("%s:删除无需再查询信息的视频列表数据失败！"%time.ctime())
    #         return 0
    result = StoreDB("DELETE FROM %s WHERE uid NOT IN (SELECT uid FROM uid_list);"%table)
    if result == 1:
        logging.info("没有需要删除/成功删除无需再查询信息的UP的视频列表数据！")
    else:
        logging.info("删除无需再查询信息的UP的视频列表数据失败！")
    ##### 查询需要爬取数据的UP的数据
    result = QueryDB("SELECT uid FROM uid_list;")
    for i in result:
        result = GetVideoList(i[0],Store=True)
        if result == 0:
            break
        else:
            pass

########## 根据bvid获取视频的详细数据 ##########
def GetVideoData(Store=False):
    ##### 变量
    table = "videos_data"
    table_ = "videos_list"
    ##### 查询videos_list中的bvid
    result = QueryDB("SELECT bvid FROM %s;"%table_)
    bv_list = [i[0] for i in result ]
    ##### 删除videos_data中（bvid）存在，但videos_list中（bvid）不存在的视频数据
    result = QueryDB("SELECT bvid FROM %s WHERE NOT EXISTS (SELECT bvid FROM %s WHERE %s.bvid=%s.bvid) GROUP BY bvid;"%(table,table_,table_,table))
    cmd = ["DELETE FROM %s WHERE bvid = '%s';" % (table, i[0]) for i in result]
    result = StoreDB(*cmd)
    if result == 1:
        logging.info("成功删除无需再查询信息的视频详细数据！")
    else:
        logging.info("删除无需再查询信息的视频详细数据失败！")
        return 0
    ##### 爬取视频数据
    url = "https://api.bilibili.com/x/web-interface/view"     #获取指定视频的数据
    para = {
        "bvid":""
    }
    header = {
        "User-Agent": "Mozilla / 5.0(Windows NT 10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.122Safari / 537.36"
    }
    VideoDataList = []
    try:
        for i in range(0,len(bv_list)):
            para["bvid"] = bv_list[i]
            resp = GetPage(url,Data=para,Headers=header)
            time.sleep(0.5)
            data = json.load(resp)
            VideoDataList.append({
                "bvid":data["data"]["bvid"],                   #视频的bv号
                "view":data["data"]["stat"]["view"],           #视频播放量
                "danmuku":data["data"]["stat"]["danmaku"],     #视频弹幕数量
                "like":data["data"]["stat"]["like"],           #点赞数量
                "coin":data["data"]["stat"]["coin"],           #投币数量
                "favorite":data["data"]["stat"]["favorite"],   #收藏数量
                "share":data["data"]["stat"]["share"],         #分享数量
                "reply":data["data"]["stat"]["reply"],          #评论数量
                "time":round(time.mktime(time.strptime((resp.getheaders())[0][1],"%a, %d %b %Y %H:%M:%S GMT")) + 3600 * 8)                       #时间
            })
            # ProgressBar( (i+1) / len(bv_list),end_str="总视频数为%d！"%len(bv_list))
        # print("\n")
    except BaseException as e:
        logging.error("爬取视频详细数据出错，请检查爬虫响应数据！%s"%e)
        return 0
    ##### 将视频详细数据存入数据库
    if Store:
        cmd = "INSERT INTO %s(bvid,view,danmuku,likes,coin,favorite,share,reply,time) VALUES "%table
        for i in VideoDataList:
            cmd = cmd + "('%s',%s,%s,%s,%s,%s,%s,%s,%s),"%(i["bvid"],i["view"],i["danmuku"],
                i["like"],i["coin"],i["favorite"],i["share"],i["reply"],i["time"])
        result = StoreDB(cmd[:-1]+";")
        if result == 1:
            logging.info("存储视频详细数据成功！")
            return 1
        else:
            logging.info("存储视频详细数据失败！")
            return 0
    else:
        return VideoDataList
