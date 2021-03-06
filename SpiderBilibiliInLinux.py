# -*- coding: utf-8 -*-
import time,http.cookiejar,logging,signal,sys,os,MyFunc
from urllib import request
from apscheduler.schedulers.background import BackgroundScheduler

######### 关闭程序处理函数 ##########
def func(signum,frame):
    if signum == signal.SIGTERM:
        sche.shutdown()
        logging.warning("程序正常退出！")
        sys.exit()

if __name__ == '__main__':
    ######### 关闭程序处理函数注册 ##########
    for i in [signal.SIGTERM]: #,signal.SIGUSR1,signal.SIGUSR2]:
        signal.signal(i,func)
    
    ########## 处理参数，实现通过一个代码文件控制程序的启动停止重启的功能 ##########
    cmd = "ps -aux | grep python | awk '{for(i=1;i<=NF;i++)if($i==\"%s\")print $2;}'"%sys.argv[0]  #’sys.arg[0]‘即为下文的‘该文件’
    ps_pid = [int(x) for x in ((os.popen(cmd)).read()).split('\n') if x!='']  #使用ps、ps、awk命令找出所有通过该文件启动的进程的pid
    my_pid = os.getpid()
    if len(sys.argv)>1:
        if sys.argv[1] == "start":  #启动命令，需要检查是否有通过该文件启动的其他进程，有则本进程自动结束，没有则该进程继续运行
            for i in ps_pid:
                if i != my_pid:
                    print("an instance process has ben started through this file,and the process will exit automatically.")
                    sys.exit(0)
        elif sys.argv[1] == "stop":  #关闭命令，关闭所有通过该文件启动的进程，本进程自动结束
            for i in ps_pid:
                if i != my_pid:
                    os.kill(i,signal.SIGTERM)
            print("other processes started through this file have been closed,and the process will exit automatically.")
            sys.exit(0)
        elif sys.argv[1] == "restart":  #重启命令，关闭本进程以外的其它通过该文件启动的进程，本进程继续运行
            for i in ps_pid:
                if i != my_pid:
                    os.kill(i,signal.SIGTERM)
            print("other processes started through this file have been closed,and the process will start automatically.")
        else:  #命令书写错误
            print("the parameters is false,please enter the correct parameters(start,stop,restart).")
            sys.exit(0)
    else:  #没有输入命令
        print("""please enter parameters!
                1. start    start the program.
                2. stop     stop the program,kill all processes.
                3. restart  restart the program,kill all other processes.""")
        sys.exit(0)

    ########## 配置日志记录 ##########
    logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d]- pid:%(process)d - %(levelname)s: %(message)s',
                        level=logging.INFO, filename="log.txt", filemode="a")
    
    ########## 构造cookie ##########
    cookie = http.cookiejar.MozillaCookieJar()
    NewOpener = request.build_opener(request.HTTPCookieProcessor(cookie))
    request.install_opener(NewOpener)  # 安装NewOpener实例作为默认全局启动器


    ########## 配置调度器 ##########
    sche = BackgroundScheduler(timezone="Asia/Shanghai")
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    ########## 定时从数据库中取出需要查询视频数据的up主的uid并更新up_info表 / 每1,11,21,31,41,51分钟查询一次
    sche.add_job(MyFunc.GetUPinfo,"cron",minute="1/10",kwargs={"Store":True})

    ########## 定时更新fans表 / 每3,13,23,33,43,53分钟查询一次
    sche.add_job(MyFunc.GetFans,"cron",minute="3/10",kwargs={"Store":True})

    ########## 定时更新videos_list表 / 每5,20,35,50分钟查询一次
    sche.add_job(MyFunc.GetVideoListNew,"cron",minute="5/15")

    ########## 定时更新videos_data表 / 每10,25,40,55分钟查询一次
    sche.add_job(MyFunc.GetVideoData,"cron",minute="10/15",kwargs={"Store":True})

    sche.start()
    while True:
        time.sleep(10)
