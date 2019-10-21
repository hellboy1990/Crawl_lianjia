
###---@LJ出品，转载请注明---###
'''
此程序为爬取链家网上各城市的小区及其房屋信息，包括图片及文字部分
'''
import pprint
import requests
from requests import RequestException
from bs4 import BeautifulSoup
import time
import pandas as pd
import re
import os
import json
from headers import get_headers
import random
import math
import time
tim = random.randint(3,5)

def req(url):#定义请求函数
    headers = {'User-Agent': get_headers()}
    webdata = requests.get(url, headers=headers, timeout=30)
    # print(webdata)
    return webdata

def get_district():#获取北京各城区链接
    global dis_links,districts
    districts=[]
    dis_links=[]
    url = 'https://bj.lianjia.com/xiaoqu/'
    webdata=req(url)
    if webdata.status_code == 200:
        soups = BeautifulSoup(webdata.text, 'lxml')
        dists = soups.select('dl dd div a')
        for dist, link in [[i.get_text(), domain + i.get('href')] for i in dists[:-2]]:
            #print(dist, link)
            districts.append(dist)
            dis_links.append(link)
    else:
        pass

def get_areas():#获取城区的各个区域
    global areas_links,dists_list,areas_list
    dists_list=[]
    areas_list=[]
    areas_links=[]
    for i in range(0,1):
    #for i in range(0,len(dis_links)):
        webdata=req(dis_links[i])
        if webdata.status_code == 200:
            soups=BeautifulSoup(webdata.text,'lxml')
            areas=soups.select('dl dd div div:nth-of-type(2) a')
            #print(areas)
            for area in areas:
                areas_list.append(area.get_text())
                areas_links.append(domain+area.get('href'))
                dists_list.append(districts[i])#添加城区信息
        else:
            pass
        time.sleep(tim*1.2)    #print(areas_list,areas_links,dists_list)

def get_xiaoqu(file_xq):#获取各个区域的小区
    xq_dist=[]#小区城区
    xq_area=[]#小区区域
    xq_titles=[]#小区名称
    xq_links=[]#小区链接
    for i in range(0,1):
    #for i in range(0,len(areas_links)):#构造区域循环
        print(areas_list[i],areas_links[i])
        webdata=req(areas_links[i])
        if webdata.status_code == 200:
            soups = BeautifulSoup(webdata.text, 'lxml')
            xq_pages = soups.select('div.page-box.house-lst-page-box')
            #print(len(xq_pages),type(json.loads(xq_pages[0].get('page-data'))['totalPage']))#页数是整型
            '''判断共有几页，如果有一页则直接爬取链接，否则先确定页数再爬取链接'''
            if len(xq_pages) == 0:  # 如果只有一页
                print(areas_list[i]+'有一页！')
                xqs= soups.select(' li.clear.LOGCLICKDATA div.info div.title a')
                for xq in xqs:
                    xq_title = xq.get_text()
                    xq_link = xq.get('href')
                    xq_titles.append(xq_title)
                    xq_links.append(xq_link)
                    xq_dist.append(dists_list[i])
                    xq_area.append(areas_list[i])
            else:  # 大于一页
                xq_pages = json.loads(xq_pages[0].get('page-data'))['totalPage']
                print(areas_list[i]+'有'+str(xq_pages)+'页！')
                for page in range(1, int(xq_pages)+1):# 构造翻页循环
                    xq_url =areas_links[i]+'pg'+str(page)
                    print(xq_url)
                    res=req(xq_url)
                    try:
                        res_soups=BeautifulSoup(res.text,'lxml')
                        xqs=res_soups.select('li.clear div.info div.title a')#定位小区信息列表
                        for xq in xqs:
                            xq_title=xq.get_text()
                            xq_link=xq.get('href')
                            xq_titles.append(xq_title)
                            xq_links.append(xq_link)
                            xq_dist.append(dists_list[i])
                            xq_area.append(areas_list[i])
                    except:
                        continue
        else:
            pass
        time.sleep(tim*1.5)
    #print(xq_titles,xq_links,xq_dist,xq_area)
    df=pd.DataFrame({'title':xq_titles,'link':xq_links,'area':xq_area,'dist':xq_dist})
    print(df.head())
    df.to_excel(file_xq,encoding='uft-8')
    print('已爬取城市区域内所有小区链接！')

def get_zs_cj():#获取小区已成效和在售的全部房屋链接
    global links_zaishou,links_chengjiao,xqnames
    xq_infos= pd.read_excel(file_xq)
    dfs=xq_infos['link']
    names=xq_infos['title']
    links_zaishou=[]#在售链接
    links_chengjiao=[]#成交链接
    xqnames=[]#小区名称
    xq_unitprices = []  # 小区均价
    xq_pricetime = []  # 参考月份
    xq_jznd = []  # 建筑年代
    xq_jzlx = []  # 建筑类型
    xq_wyfy = []  # 物业费用
    xq_wygs = []  # 物业公司
    xq_kfs = []  # 开发商
    xq_ldzs = []  # 楼栋总数
    xq_fwzs = []  # 房屋总数
    imgs_links = []
    for i in range(0,1):#构造小区循环
    #for i in range(0, len(dfs)):
        print(dfs[i])
        try:
            webdata=req(dfs[i])
            if webdata.status_code==200:
                soups=BeautifulSoup(webdata.text,'lxml')
                link_zaishou=soups.select('div.goodSell a.fr')[0].get('href')
                #print(link_zaishou)
                links_zaishou.append(link_zaishou)#建立所有在售列表
                link_chengjiao = soups.select('div.frameDeal a.btn-large')[0].get('href')
                #print(link_chengjiao)
                links_chengjiao.append(link_chengjiao)#建立所有成交列表
                xqnames.append(names[i])
                #价格信息
                unitprice = soups.select('div.xiaoquPrice span')[0].text.strip()
                xq_unitprices.append(unitprice)
                #print(unitprice)
                pricetime = soups.select('div.xiaoquPrice span')[1].text.strip()
                xq_pricetime.append(pricetime)
                #基本信息
                jznd = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[0].text.strip()
                xq_jznd.append(jznd)
                #print(jznd)
                jzlx = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[1].text.strip()
                xq_jzlx.append(jzlx)
                wyfy = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[2].text.strip()
                xq_wyfy.append(wyfy)
                wygs = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[3].text.strip()
                xq_wygs.append(wygs)
                kfs = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[4].text.strip()
                xq_kfs.append(kfs)
                ldzs = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[5].text.strip()
                xq_ldzs.append(ldzs)
                fwzs = soups.select('div.xiaoquInfoItem span.xiaoquInfoContent')[6].text.strip()
                xq_fwzs.append(fwzs)
                # 图片链接
                img_link = soups.select('ol#overviewThumbnail li')#图片列表
                img_links=[link.get('data-src') for link in img_link]
                imgs_links.append(img_links)
        except:
            continue
        time.sleep(tim*1.2)
    #print(len(xqnames),len(imgs_links),len(xq_fwzs))
    df_xq_info=pd.DataFrame({'names':xqnames,'unitprice':xq_unitprices,'pricetime':xq_pricetime,
                               'jianzhuniandai':xq_jznd,
                               'jianzhuleixing':xq_jzlx,'wuyefeiyong':xq_wyfy,'wuyegongsi':xq_wygs,
                               'kaifashang':xq_kfs,
                               'loudongzongshu':xq_ldzs,'fangwuzongshu':xq_fwzs,
                               'zaishoulianjie':links_zaishou,'chengjiaolianjie':links_chengjiao,
                               'imags_links':imgs_links})
    print(df_xq_info.head())
    df_xq_info.to_csv(os.getcwd() + file_xq_info, index=False, sep=',', encoding='utf-8')
    print('已爬取让所有小区信息及图片链接！')

def get_zs_link():#获取小区在售房屋的所有链接
    zs_links=[]
    for i in range(0, len(links_zaishou)):  # 构造小区在售链接循环
        print(links_zaishou[i])
        webdata = req(links_zaishou[i])
        if webdata.status_code == 200:
            soups = BeautifulSoup(webdata.text, 'lxml')
            zs_pages=soups.select('div.page-box.house-lst-page-box')
            '''判断共有几页，如果有一页则直接爬取链接，否则先确定页数再爬取链接'''
            if len(zs_pages)==0:#如果只有一页
                print(xqnames[i] + '有一页！')
                zs_urls=soups.select('ul.sellListContent li.clear.LOGCLICKDATA div.title a')
                for zs_url in zs_urls:
                    zs_links.append(zs_url.get('href'))
            else:#大于一页
                zs_pages = json.loads(zs_pages[0].get('page-data'))['totalPage']
                print(xqnames[i]+'有'+str(zs_pages)+'页！')
                for page in range(1,zs_pages+1):#构造翻页循环
                    zs_page = 'ershoufang/pg' + str(page)
                    zs_link = links_zaishou[i].replace('ershoufang/', zs_page)
                    zs_res = req(zs_link)
                    try:
                        cj_soups = BeautifulSoup(zs_res.text, 'lxml')
                        zs_urls = cj_soups.select('ul.sellListContent li.clear.LOGCLICKDATA div.title a')
                        for zs_url in zs_urls:  # 构造成效房屋循环
                            zs_links.append(zs_url.get('href'))
                    except:
                        continue
                    time.sleep(tim)
        else:
            pass
        time.sleep(tim)
    #print(zs_links,len(zs_links))
    df=pd.DataFrame({'在售链接':zs_links})
    print(df.head())
    df.to_excel(os.getcwd()+file_zs)
    print('已爬取所有小区在售住房链接！')

def get_cj_link():#获取小区成交房屋的所有链接
    cj_links=[]
    for i in range(0,len(links_chengjiao)):#构造成交页循环
        print(links_chengjiao[i])
        webdata=req(links_chengjiao[i])
        if webdata.status_code==200:
            soups=BeautifulSoup(webdata.text,'lxml')
            cj_pages=soups.select('div.page-box.house-lst-page-box')
            #print(cj_pages)
            '''判断共有几页，如果有一页则直接爬取链接，否则先确定页数再爬取链接'''
            if len(cj_pages) == 0:  # 如果只有一页
                print(xqnames[i] + '有一页！')
                cj_urls = soups.select('li div.info div.title a')
                for cj_url in cj_urls:
                    cj_links.append(cj_url.get('href'))
            else:
                cj_pages = json.loads(cj_pages[0].get('page-data'))['totalPage']
                print(xqnames[i] + '有' + str(cj_pages) + '页！')
                for page in range(1,cj_pages+1):#构造翻页循环
                    cj_page='chengjiao/pg'+str(page)
                    cj_link=links_chengjiao[i].replace('chengjiao/',cj_page)
                    cj_res=req(cj_link)
                    try:
                        cj_soups=BeautifulSoup(cj_res.text,'lxml')
                        cj_urls=cj_soups.select('li div.info div.title a')
                        for cj_url in cj_urls:#构造成交房屋循环
                            cj_links.append(cj_url.get('href'))
                    except:
                        continue
                    time.sleep(tim)
        else:
            pass
        time.sleep(tim)
    #print(cj_links,len(cj_links))
    df = pd.DataFrame({'成交链接': cj_links})
    print(df.head())
    df.to_excel(os.getcwd() + file_cj)
    print('已爬取所有小区成交住房链接！')

def get_zs_house():
    info_jznds = []  #建筑年代
    xqnms = []  # 小区名称
    totals = []  # 总价
    unitprices = []  # 单价
    info_fwhxs = []  # 房屋户型
    info_fwlcs = []  # 所在楼层
    info_fwmjs = []  # 建筑面积
    info_hxjgs = []  # 户型结构
    info_tnmjs = []  # 套内面积
    info_leixs = []  # 建筑类型
    info_chaos = []  # 房屋朝向
    info_jzjgs = []  # 建筑结构
    info_zxius = []  # 装修情况
    info_thbls = []  # 梯户比例
    info_gnfss = []  # 供暖方式
    info_pbdts = []  # 配备电梯
    info_cqnxs = []  # 产权年限
    # tr_gpsjs = []  # 挂牌时间
    # tr_jyqss = []  # 交易权属
    # tr_scjys = []  # 上次交易
    # tr_fwyts = []  # 房屋用途
    # tr_fwnxs = []  # 房屋年限
    # tr_cqsss = []  # 产权所属
    # tr_dyxxs = []  # 抵押信息
    # tr_fbbjs = []  # 房本备件
    house_link=pd.read_excel(file_zs)['在售链接']
    for i in range(0,len(house_link)):
        print(house_link[i])
        webdata=req(house_link[i])
        if webdata.status_code==200:
            soups = BeautifulSoup(webdata.text, 'lxml')
            try:
                total=soups.select('div.price span.total')[0].text.strip()#总价
                totals.append(total)
                #print(totals)
                unitprice=soups.select('div.price div.unitPrice span')[0].text.strip()#单价
                unitprices.append(unitprice)
                info_jznd = soups.select('div.area div.subInfo')[0].text.strip()#建筑年代
                info_jznds.append(info_jznd)
                xqnm= soups.select('div.aroundInfo div.communityName')#小区名称
                xqnms.append(xqnm)
                #基本信息
                info_fwhx = soups.select('div.base div.content ul li span')[0].next_sibling#户型
                info_fwhxs.append(info_fwhx)
                # print(info_fwhxs)
                info_fwlc = soups.select('div.base div.content ul li span')[1].next_sibling#楼层
                info_fwlcs.append(info_fwlc)
                info_fwmj = soups.select('div.base div.content ul li span')[2].next_sibling
                info_fwmjs.append(info_fwmj)
                info_hxjg = soups.select('div.base div.content ul li span')[3].next_sibling
                info_hxjgs.append(info_hxjg)
                info_tnmj = soups.select('div.base div.content ul li span')[4].next_sibling
                info_tnmjs.append(info_tnmj)
                info_leix = soups.select('div.base div.content ul li span')[5].next_sibling
                info_leixs.append(info_leix)
                info_chao = soups.select('div.base div.content ul li span')[6].next_sibling
                info_chaos.append(info_chao)
                info_jzjg = soups.select('div.base div.content ul li span')[7].next_sibling
                info_jzjgs.append(info_jzjg)
                info_zxiu = soups.select('div.base div.content ul li span')[8].next_sibling
                info_zxius.append(info_zxiu)
                info_thbl = soups.select('div.base div.content ul li span')[9].next_sibling
                info_thbls.append(info_thbl)
                info_gnfs = soups.select('div.base div.content ul li span')[10].next_sibling
                info_gnfss.append(info_gnfs)
                info_pbdt = soups.select('div.base div.content ul li span')[11].next_sibling
                info_pbdts.append(info_pbdt)
                info_cqnx = soups.select('div.base div.content ul li span')[12].next_sibling
                info_cqnxs.append(info_cqnx)
            except:
                continue
        time.sleep(tim*1.2)
    print(len(xqnms),len(info_chaos))
    # 写入文件
    df_house = pd.DataFrame({'name': xqnms, 'tot': totals, 'unit': unitprices, 'jznt': info_jznds, 'fwhx': info_fwhxs,
                       'fwlc': info_fwlcs, 'fwmj': info_fwmjs, 'hxjg': info_hxjgs, 'tnmj': info_tnmjs,
                       'leixin': info_leixs, 'chaoxiang': info_chaos, 'jzjg': info_jzjgs, 'zhuangxiu': info_zxius,
                       'tnbl': info_thbls, 'gnfs': info_gnfss, 'pbdt': info_pbdts, 'cqnx': info_cqnxs,})
    print(df_house.head())
    df_house.to_csv(os.getcwd()+file_zs_house, index=False, sep=',', encoding='utf-8')
    print('已爬取所有在售房屋信息！')

def get_cj_house():
    dealprices=[]#交易总价
    msg_guapai=[]#挂牌价格
    msg_chengjiao=[]
    msg_tiaojia=[]
    msg_daikan=[]
    msg_guanzhu=[]
    msg_liulan=[]
    info_fwhxs = []  # 房屋户型
    info_fwlcs = []  # 所在楼层
    info_fwmjs = []  # 建筑面积
    info_hxjgs = []  # 户型结构
    info_tnmjs = []  # 套内面积
    info_leixs = []  # 建筑类型
    info_chaos = []  # 房屋朝向
    info_jzjgs = []  # 建筑结构
    info_zxius = []  # 装修情况
    info_thbls = []  # 梯户比例
    info_gnfss = []  # 供暖方式
    info_pbdts = []  # 配备电梯
    info_cqnxs = []  # 产权年限
    house_link = pd.read_excel(file_cj)['成交链接']
    for i in range(0,2):
    #for i in range(0, len(house_link)):
        try:
            print(house_link[i])
            webdata = req(house_link[i])
            if webdata.status_code == 200:
                soups = BeautifulSoup(webdata.text, 'lxml')
                dealprice= soups.select('div.price span.dealTotalPrice')[0].text.strip()  # 交易总价
                dealprices.append(dealprice)
                guapai=soups.select('div.info.fr div.msg span')[0].text.strip()
                msg_guapai.append(guapai)
                chengjiao=soups.select('div.info.fr div.msg span')[1].text.strip()
                msg_chengjiao.append(chengjiao)
                tiaojia=soups.select('div.info.fr div.msg span')[2].text.strip()
                msg_tiaojia.append(tiaojia)
                daikan= soups.select('div.info.fr div.msg span')[3].text.strip()
                msg_daikan.append(daikan)
                guanzhu = soups.select('div.info.fr div.msg span')[4].text.strip()
                msg_guanzhu.append(guanzhu)
                liulan= soups.select('div.info.fr div.msg span')[5].text.strip()
                msg_liulan.append(liulan)
                # 基本信息
                info_fwhx = soups.select('div.base div.content ul li span')[0].next_sibling  # 户型
                info_fwhxs.append(info_fwhx)
                info_fwlc = soups.select('div.base div.content ul li span')[1].next_sibling  # 楼层
                info_fwlcs.append(info_fwlc)
                info_fwmj = soups.select('div.base div.content ul li span')[2].next_sibling
                info_fwmjs.append(info_fwmj)
                info_hxjg = soups.select('div.base div.content ul li span')[3].next_sibling
                info_hxjgs.append(info_hxjg)
                info_tnmj = soups.select('div.base div.content ul li span')[4].next_sibling
                info_tnmjs.append(info_tnmj)
                info_leix = soups.select('div.base div.content ul li span')[5].next_sibling
                info_leixs.append(info_leix)
                info_chao = soups.select('div.base div.content ul li span')[6].next_sibling
                info_chaos.append(info_chao)
                info_jzjg = soups.select('div.base div.content ul li span')[7].next_sibling
                info_jzjgs.append(info_jzjg)
                info_zxiu = soups.select('div.base div.content ul li span')[8].next_sibling
                info_zxius.append(info_zxiu)
                info_thbl = soups.select('div.base div.content ul li span')[9].next_sibling
                info_thbls.append(info_thbl)
                info_gnfs = soups.select('div.base div.content ul li span')[10].next_sibling
                info_gnfss.append(info_gnfs)
                info_pbdt = soups.select('div.base div.content ul li span')[11].next_sibling
                info_pbdts.append(info_pbdt)
                info_cqnx = soups.select('div.base div.content ul li span')[12].next_sibling
                info_cqnxs.append(info_cqnx)
        except:
            continue
        time.sleep(tim * 1.2)
    print(len(dealprices), len(info_chaos))
    # 写入文件
    df_house = pd.DataFrame({'dealprice': dealprices, 'guapai': msg_guapai, 'chengiaozhouqi': msg_chengjiao,
                             'tiaojia': msg_tiaojia,
                             'daikan': msg_daikan,'guanzhu':msg_guanzhu,'liulan':msg_liulan,
                             'fwlc': info_fwlcs, 'fwmj': info_fwmjs, 'hxjg': info_hxjgs, 'tnmj': info_tnmjs,
                             'leixin': info_leixs, 'chaoxiang': info_chaos, 'jzjg': info_jzjgs, 'zhuangxiu': info_zxius,
                             'tnbl': info_thbls, 'gnfs': info_gnfss, 'pbdt': info_pbdts, 'cqnx': info_cqnxs, })
    print(df_house.head())
    df_house.to_csv(os.getcwd() + file_cj_house, index=False, sep=',', encoding='utf-8')
    print('已爬取所有在售房屋信息！')


if __name__=="__main__":
    #第一步爬取所有城区的区域内的所有小区并保存到列表
    domain = 'https://bj.lianjia.com'
    file_xq= os.getcwd() + './xq_infos.xls'
    # get_district()
    # get_areas()
    # get_xiaoqu(file_xq)

    # #第二步爬取所有小区房屋的成交和在售列表，以及图片链接
    file_xq_info='./xq_info.csv'
    file_zs = './zs_links.xls'
    file_cj='./cj_links.xls'
    get_zs_cj()
    get_zs_link()
    get_cj_link()

    #第三步是爬取所有房屋信息
    file_zs_house='./zs_house.csv'
    file_cj_house='./cj_house.csv'
    get_zs_house()
    get_cj_house()