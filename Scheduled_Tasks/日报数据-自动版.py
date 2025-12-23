import numpy as np
import pandas as pd
import xlwings as xw
from openpyxl import load_workbook
import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
from apscheduler.schedulers.background import BackgroundScheduler

import warnings
warnings.filterwarnings("ignore")

import pymysql
from sqlalchemy import create_engine
import json
import time
from datetime import timedelta , datetime, timezone
from dateutil.relativedelta import relativedelta
import re
import os
import glob
import sys

from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class Report_Day:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        # 实例化All_Model类
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()
        self.Today = str(datetime.now().strftime('%Y%m%d%H'))

    # 连接数据库
    def query(self, sql,
            host="rm-wz930e5269fur1ht1mo.mysql.rds.aliyuncs.com",
            user="wxz",
            password="5JRcY9SaiepVlIq7iuPo",
            database='',
            port=3306
            ):
        conn = pymysql.connect(
            host=host,
            user=user,
            port=port,
            password=password,
            max_allowed_packet=1073741824,
            charset="utf8")
        try:
            df = pd.read_sql(sql, con=conn)
            conn.close()
        except:
            print('error')
            conn.close()
            raise
        return df

    # 查询数据
    def select_data(self):
        sql1 = ''' -- 订单&风控信息  近10日数据   
        SELECT date(om.create_time) as create_date,om.create_time,om.id as order_id ,om.order_number,om.all_money 
        ,om.status, om.user_id
        ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
        when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
        when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as status2 
        ,case when locate('租物',pa.name)>0 or locate('租物',cc.name)>0 or locate('芝麻',pa.name)>0 or locate('芝麻',cc.name)>0  then '芝麻租物' when locate('抖音',pa.name)>0 then '抖音渠道' when locate('搜索',cc.name)>0 then '搜索渠道' else '其他渠道' end as channel_type 
        ,tod.sku_attributes,tod.product_name,tod.new_actual_money
        ,case when  locate('租完即送',tod.sku_attributes)>0 then '租完即送' else '租物归还' end as back_type
        ,om.user_mobile,tmu.true_name,tmu.id_card_num
        ,top.total_describes,tor.decision_result,om.cancel_reason
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.traceid') end,'"','') as trace_id 
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.rejected') end,'"','') as rejected 
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.result') end,'"','') as result 
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.tips') end,'"','') as tips  
        ,cc.name as channel_name         -- 来源渠道
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_id,om.merchant_name
        ,topay.total_freeze_fund_amount , tod.dy_order_item_json, pa.type
        ,om.buy_service_product,tso.status as service_status 
        ,om.order_method, om.activity_id, om.appid, tprm.max_overdue_days
        , tor.update_time, tomt.reason
        from  db_digua_business.t_order  om
        left join db_digua_business.t_postlease_receivables_monitoring tprm on tprm.order_id=om.id
        left join db_digua_business.t_order_risk tor on om.id = tor.order_id
        -- 备注信息合并 
        left join ( SELECT  t.order_id,JSON_ARRAYAGG(t.describes) as total_describes from db_digua_business.t_order_personnel t   GROUP BY 1 ) top 
        on om.id = top.order_id 
        -- 服务信息
        left join  db_digua_business.t_service_order tso  on om.id = tso.order_id 
        -- 渠道名称
        left join db_digua_business.t_channel cc on om.channel = cc.scene 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        -- 用户信息 
        left join db_digua_business.t_member_user tmu on om.user_id = tmu.id
        -- 商品信息
        left join db_digua_business.t_order_details tod on om.id = tod.order_id
        -- 免押信息  
        left join (SELECT t.*,row_number() over(partition by t.order_id order by t.pay_date desc) as rn 
        from db_digua_business.t_order_pay t 
        
        where t.pay_type = 'ZFBYSQ' and t.item_type=1 and t.`status` in (2,5) and t.trade_no is not null )  topay 
        on topay.order_id=om.id   and  topay.rn = 1 
        -- 商家订单转移表
        left join db_digua_business.t_order_merchant_transfer tomt on tomt.order_id=om.id
        
        where om.user_mobile is not null 
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        -- and pa.type!=4
        and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -15 day )
        -- and hour(om.create_time)<16 
        ;
        '''
        sql3 = ''' -- 拒量拒绝原因
        SELECT id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status   FROM `db_credit`.risk_examine
        '''
        # sql_ck = ''' -- 台账出库数据
        # select date, order_number, category, remark from db_digua_business.t_ledger
        # '''
        f_path_ck = "F:/myfile/p站数据/台账数据/维客壹佰2025年台账.xlsx"
        df_ck = pd.read_excel(f_path_ck, sheet_name="2025")
        df_order = self.query(sql1)
        df_order = df_order[df_order.type!=4]
        df_risk_examine = self.query(sql3)
        # df_ck = self.query(sql_ck)
        return df_order, df_risk_examine, df_ck

    # 渠道归属
    def qudao_type(self, a, b, c):
        a = str(a)
        b = str(b)
        if "租物" in b:
            return "芝麻租物"
        elif "芝麻" in b:
            return "芝麻租物"
        elif "抖音" in b:
            return "抖音渠道"
        elif "搜索" in a:
            return "搜索渠道"
        elif "租物" in a:
            return "芝麻租物"
        elif "芝麻" in a:
            return "芝麻租物"
        elif c == 1:
            return "芝麻租物"
        elif "叮咚直播" in a:
            return "叮咚直播"
        elif "租瓜直播2号" in a:
            return "租瓜直播2号"
        elif "租瓜直播" in a:
            return "租瓜直播"
        elif "直播" in a:
            return "支付宝直播"
        elif "繁星" in a:
            return "繁星"
        elif "生活号" in a:
            return "生活号"
        elif "群" in a:
            return "支付宝社群"
        elif "付费灯火" in a:
            return "付费灯火"
        else:
            return a

    # 数据处理
    def clean_data(self, df, df_ck):
        # 处理日期
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        df["下单月份"] = df["create_time"].dt.strftime('%Y-%m')
        df['hour'] = df['create_time'].dt.hour
        df['minute'] = df['create_time'].dt.minute
        df['拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        # 处理备注信息
        df = df[df['sku_attributes'].notnull()]
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()
        df.loc[:, "商品类型"] = np.where(
            df["product_name"].str.contains('99新') | df["product_name"].str.contains('95新') | df[
                "product_name"].str.contains('准新'), "二手", "全新")  ##  准新
        df.loc[:, "租赁方案"] = np.where(df["sku_attributes"].str.contains('租完即送'), "租完即送", "租完归还")
        df.loc[:, "押金类型"] = np.where(df["total_freeze_fund_amount"] > 0, "部分免押", "全免押")
        df.loc[:, "优惠券使用否"] = np.where(
            (df["new_actual_money"] - df["all_money"] > 0) & (df["租赁方案"] == '租完即送'), "已使用", "未使用")
        df.loc[:, "是否进行预授权"] = np.where(df["total_freeze_fund_amount"].isnull(), "未预授权", "已预授权")
        # 获取颜色
        def getcolor(s):
            color_list = json.loads(s)
            for j in range(0, len(color_list)):
                if color_list[j]["key"] == "颜色":
                    return color_list[j]["value"]
        # 获取内存
        def getneicun(s):
            color_list = json.loads(s)
            for j in range(0, len(color_list)):
                if color_list[j]["key"] == "内存":
                    return color_list[j]["value"]

        df.loc[:, "颜色"] = df.apply(lambda x: getcolor(x["sku_attributes"]), axis=1)
        df.loc[:, "内存"] = df.apply(lambda x: getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(lambda x: self.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"]),axis=1)
        # 订单去重
        dict_status_code = {
            "订单取消": 1,
            "待支付": 2,
            "已退款": 3,
            "待审核": 4,
            "待发货": 5,
            "待收货": 6,
            "租赁中": 7,
            "已完成": 8
        }
        df["状态编码"] = df["status2"].map(dict_status_code)

        df.sort_values(by=["下单日期", "状态编码"], inplace=True)

        # 删除重复单号
        df.drop_duplicates(subset=["order_id"], inplace=True)
        # 删除身份证空值行
        df.dropna(subset=["id_card_num"], axis=0, inplace=True)
        # 去刷单订单
        df.drop(df[df['total_describes'].str.contains(pat='panli', regex=False) == True].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单秘密计划"].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单秘密计划-无优惠"].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单曙光计划"].index, inplace=True)
        df.drop(df[df['activity_name'] == "线下门店3个月试行"].index, inplace=True)
        # 删除状态空值行
        df.dropna(subset=["status2"], axis=0, inplace=True)

        # 删除重复订单
        df.drop_duplicates(subset=["order_id"], inplace=True)
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)

        df.drop(df[df['true_name'].isin(
            ["刘鹏", "谢仕程", "潘立", "洪柳", "陈锦奇", "周杰", "卢腾标", "孔靖", "黄娟", "钟福荣", "邱锐杰", "唐林华"
                , "邓媛斤", "黄子南", "刘莎莎", "赖瑞彤", "孙子文"])].index, inplace=True)
        # 定义状态
        # 判断 前置拦截   机审强拒   出库前风控强拒
        df['是否前置拦截'] = np.where(
            (df.result.str.contains('id_card不得为空')) | (df.result.str.contains('mobile校验不通过')) | (
                df.result.str.contains('name校验不通过'))
            | (df.result.str.contains('年龄超过49岁或低于18岁')) | (df.result == '风控未通过') | (
                        df.result == '黑名单用户'), 1, 0)
        df['是否机审强拒'] = np.where(
            (df.result.str.contains('风控拒绝') & (~df.result.str.contains('命中出库前风控流强拒').fillna(False))), 1,
            0)
        df['是否出库前风控强拒'] = np.where((df.result.str.contains('命中出库前风控流强拒').fillna(False)) | (
                    (df.total_describes.str.contains('蚂蚁数控风险等级').fillna(False)) & (
                ~df.result.str.contains('黑名单用户').fillna(False))), 1, 0)
        df.loc[:,"审核状态"]=df.apply(lambda x: self.clean.reject_type(x["拒绝理由"],x["进件"],x["电审拒绝原因"],x["取消原因"],x["status2"],x["无法联系原因"],x["total_describes"],x['是否前置拦截'],x['是否机审强拒'],x['是否出库前风控强拒']),axis=1)
        # 保留商家数据
        df_contain = df.copy()
        # 剔除商家数据
        df = self.clean.drop_merchant(df)
        # 进件数据
        df_j = df[df["进件"] == "进件"]
        # 各个节点状态
        df["待审核"] = np.where(df["审核状态"] == '待审核', 1, 0)
        df["前置拦截"] = np.where(df["审核状态"] == '前置拦截', 1, 0)
        df["人审拒绝"] = np.where(df["审核状态"] == '人审拒绝', 1, 0)
        df["客户取消"] = np.where(df["审核状态"] == '客户取消', 1, 0)
        df["无法联系"] = np.where(df["审核状态"] == '无法联系', 1, 0)
        df["是否进件"] = np.where(df["进件"] == '进件', 1, 0)
        df["是否出库"] = np.where(df["status"].isin([2, 3, 4, 5, 6, 8, 15]), 1, 0)
        df["进件前取消"] = np.where(df["审核状态"] == '进件前取消', 1, 0)
        df['是否出库'] = np.where(
            (df['人审拒绝'] == 0) & (df['客户取消'] == 0) & (df['无法联系'] == 0) & (df['待审核'] == 0) & (
                        df['是否出库'] == 1), 1, 0)
        df["出库前风控强拒"] = np.where((df["审核状态"] == '出库前风控强拒') & (df['是否出库'] == 0), 1, 0)
        df["机审强拒"] = np.where((df["审核状态"] == '机审强拒') & (df['是否出库'] == 0), 1, 0)
        df['机审通过件'] = np.where((df['是否进件'] == 1) & (df['机审强拒'] == 0), 1, 0)
        df['风控通过件'] = np.where((df['是否进件'] == 1) & (df['机审强拒'] == 0) & (df['人审拒绝'] == 0), 1, 0)
        df['已退款'] = np.where((df['风控通过件'] == 1) & (df['审核状态'] == '已退款'), 1, 0)
        df['是否二手'] = np.where(df['product_name'].str.contains(r'99新|95新|准新|90新'), 1, 0)
        # 关联台账数据
        dfck = pd.merge(df_ck, df, left_on="订单号", right_on='order_number')
        dfck.drop_duplicates(subset=["order_number"], inplace=True)
        # 删除已退款订单
        dfck.drop(dfck[dfck["status2"] == "已退款"].index, inplace=True)
        # 删除 露营设备 出库
        dfck.drop(dfck[dfck["类目"] == "露营设备"].index, inplace=True)

        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)

        return df_contain, df, df2, dfck, df_j

    # 获取出库单数
    def order_ck(self, dfck):
        # 按转化日期看每日各渠道出库单数
        df_weekday_zh = pd.crosstab(dfck["date"], dfck["归属渠道"], margins=True)
        # 按下单日期看每日各渠道出库单数（纯租物）
        df_ly_ck = pd.crosstab(dfck["下单日期"], dfck["来源渠道"], margins=True)
        # 按下单日期看每日支付宝直播商品出库单数
        df_zfb_ck = pd.crosstab(dfck["下单日期"], dfck["activity_name"], margins=True)
        # 按下单日期看免押、非免押出库单数
        df_my = pd.crosstab(dfck["下单日期"], dfck["押金类型"], margins=True)

        return df_weekday_zh, df_ly_ck, df_zfb_ck, df_my

    # 获取数据
    def get_data(self, df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel=None, name=None):
        if (channel == None) and (name == None):
            # 总体数据
            df_all = self.all_models.data_group(df, df2, df_risk_examine, model)
            df_all['出库（按转化时间）'] = df_weekday_zh['All'][:-1]
            df_all = df_all[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
                    "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
                    "出库前风控强拒", "待审核", '出库', '出库（按转化时间）', '进件出库率', '取消率',
                    '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率']]
            return df_all
        else:
            # 渠道数据
            if name == '免人审':
                df_channel = df[df[channel].str.contains(name, regex=False)==True]
                df_channel2 = df2[df2[channel].str.contains(name, regex=False)==True]
            else:
                df_channel = df[df[channel] == name]
                df_channel2 = df2[df2[channel] == name]
            df_channel_group = self.all_models.data_group(df_channel, df_channel2, df_risk_examine, model)
            try:
                if name == '支付宝直播商品':
                    df_channel_group['出库（按转化时间）'] = df_zfb_ck[name][:-1]
                elif name == '全免押' or name == '部分免押':
                    df_channel_group['出库'] = df_my[name][:-1]
                else:
                    df_channel_group['出库（按转化时间）'] = df_weekday_zh[name][:-1]
            except:
                df_channel_group['出库（按转化时间）'] = 0
            return df_channel_group

    def get_data_hour(self,df, df2, df_risk_examine, hour, minute):
        # 总体
        df_all2 = self.all_models.data_group_hour(df, df2, df_risk_examine, ['下单日期', 'hour', 'minute'], hour, minute)
        df_all2 = df_all2[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系","出库前风控强拒","待审核",'出库', '进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']]
        # 搜索渠道
        df_ss = df[df.归属渠道=='搜索渠道']
        df_ss2 = df2[df2.归属渠道 == '搜索渠道']
        df_ss_group2 = self.all_models.data_group_hour(df_ss, df_ss2, df_risk_examine, ['下单日期', 'hour', 'minute'], hour, minute)
        df_ss_group2 = df_ss_group2[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件","人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系", "出库前风控强拒", "待审核", '出库','进件出库率', '取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率']]
        # 芝麻租物
        df_zm = df[df.归属渠道 == '芝麻租物']
        df_zm2 = df2[df2.归属渠道 == '芝麻租物']
        df_zm_group2 = self.all_models.data_group_hour(df_zm, df_zm2, df_risk_examine, ['下单日期', 'hour', 'minute'], hour, minute)
        df_zm_group2 = df_zm_group2[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系","出库前风控强拒","待审核",'出库','进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']]
        # print(df_all2)
        return df_all2, df_ss_group2, df_zm_group2

    # 渠道数据
    def channel(self, df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my,  model):
        # 总体数据
        df_all = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model).reset_index()
        # 搜索渠道数据
        df_ss = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='归属渠道', name='搜索渠道')[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系",
                    "出库前风控强拒","待审核",'出库','出库（按转化时间）','进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']].reset_index()
        # 单人会话数据
        df_dr = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='来源渠道', name='单人聊天会话中的小程序消息卡片（分享）')[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例",'出库','出库（按转化时间）','进件出库率','订单出库率']].reset_index()
        # 芝麻租物数据
        df_zm = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='归属渠道', name='芝麻租物')[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系","出库前风控强拒",
        "待审核",'出库','出库（按转化时间）','进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']].reset_index()
        # 纯租物数据
        df_zw = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='来源渠道', name='芝麻信用')[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例",'出库','进件出库率','订单出库率']].reset_index()
        # 抖音渠道数据
        df_dy = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='归属渠道', name='抖音渠道')[["去重订单数","进件前取消","进件前取消率","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","客户取消","取消率","人审拒绝","人审拒绝率","待审核","出库前风控强拒",'出库','出库（按转化时间）','进件出库率','订单出库率']].reset_index()
        # 支付宝直播商品数据
        df_zfb = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='activity_name', name='支付宝直播商品')[["去重订单数","进件前取消","进件前取消率","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","人审拒绝率","风控通过件","客户取消","取消率",'无法联系',"出库前风控强拒","待审核",'出库', '出库（按转化时间）', '进件出库率','订单出库率']].reset_index()
        # 免审转化数据
        df_ms = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='decision_result', name='免人审')[["进件数","人审拒绝", "客户取消","出库","待审核","进件出库率","取消率","人审拒绝率"]].reset_index()
        # 免审订单转化数据
        df_ms_order = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='decision_result', name='免人审')[["进件数","人审拒绝","客户取消","出库","待审核","进件出库率","取消率","人审拒绝率",'出库前风控强拒','出库前强拒比例','无法联系','无法联系占比']].reset_index()
        df_ms_order.insert(0, '月份', df_ms_order['下单日期'].astype(str).str.split('-').str[0] + '-' +
                                df_ms_order['下单日期'].astype(str).str.split('-').str[1])
        # 免押订单转化率 全免押
        df_qmy = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='押金类型', name='全免押')[["进件数","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","待审核", '出库', '进件出库率', '取消率', '人审转化率', '人审拒绝率']].reset_index()
        df_qmy.insert(1, '总进件', df_all['进件数'])
        df_qmy.insert(2, '免押进件占比',(df_qmy.进件数 / df_qmy.总进件).map(lambda x: format(x, '.2%')))
        # 免押订单转化率 部分免押
        df_fmy = self.get_data(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, model, channel='押金类型', name='部分免押')[["进件数","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消", '待审核', '出库']].reset_index()
        df_fmy.insert(1, '总进件', df_fmy['进件数'])
        df_fmy.insert(2, '免押进件占比',(df_fmy.进件数 / df_fmy.总进件).map(lambda x: format(x, '.2%')))
        df_fmy.loc[:, '进件出库率'] = (df_fmy.出库 / df_fmy.进件数).map(lambda x: format(x, '.2%'))
        df_fmy.loc[:, '取消比例'] = (df_fmy.客户取消 / df_fmy.进件数).map(lambda x: format(x, '.2%'))
        df_fmy.loc[:, '人审转化率'] = (df_fmy.出库 / df_fmy.机审通过件).map(lambda x: format(x, '.2%'))
        df_fmy.loc[:, '人审拒绝率'] = (df_fmy.人审拒绝 / df_fmy.进件数).map(lambda x: format(x, '.2%'))
        return df_all, df_ss, df_dr, df_zm, df_zw, df_dy, df_zfb, df_ms, df_ms_order, df_qmy, df_fmy

    # 总体剔除直播数据
    def all_tc(self, df_all, df_dr, df_dy, df_zfb):
        df_tc = df_all[['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']] - df_dr[
            ['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']] - df_dy[
                    ['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']] - df_zfb[
                    ['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']]
        df_tc.fillna(df_all[['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']] - df_dr[
            ['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']] - df_zfb[
                        ['去重订单数', '进件数', '机审强拒', '出库', '出库（按转化时间）']], inplace=True)
        df_tc['预授权通过率'] = df_tc['进件数'] / df_tc['去重订单数']
        df_tc['预授权通过率'] = df_tc['预授权通过率'].apply(lambda x: format(x, '.2%'))
        df_tc['强拒比例'] = df_tc['机审强拒'] / df_tc['进件数']
        df_tc['强拒比例'] = df_tc['强拒比例'].apply(lambda x: format(x, '.2%'))
        df_tc['进件出库率'] = df_tc['出库'] / df_tc['进件数']
        df_tc['进件出库率'] = df_tc['进件出库率'].apply(lambda x: format(x, '.2%'))
        df_tc['订单出库率'] = df_tc['出库'] / df_tc['去重订单数']
        df_tc['订单出库率'] = df_tc['订单出库率'].apply(lambda x: format(x, '.2%'))
        df_tc = df_tc[['去重订单数', '进件数', '预授权通过率', '机审强拒', '强拒比例', '出库', '出库（按转化时间）',
            '进件出库率', '订单出库率']].reset_index()

        return df_tc

    # 免审数据整合
    def data_integration(self,df, dfck, df_all, df_ss, df_zm, df_ms):
        # 各渠道每日免审出库单数统计
        dfdfck = dfck[dfck["decision_result"].str.contains(pat="免人审", regex=False) == True]

        dfdfck_ck = pd.crosstab(dfdfck["下单日期"], dfdfck["归属渠道"], margins=True)[['芝麻租物', '搜索渠道']][:-1]
        # 删除原先索引
        dfdfck_ck = dfdfck_ck.reset_index(drop=True)
        dfdfck_ck.rename(columns={'芝麻租物': '芝麻租物免审出库', '搜索渠道': '搜索免审出库'}, inplace=True)

        # 租物、搜索的免审进件
        dfms = df[df["decision_result"].str.contains(pat="免人审", regex=False) == True]
        dfms_jj = pd.crosstab(dfms["下单日期"], dfms["归属渠道"])[:-1]
        # 删除原先索引
        dfms_jj = dfms_jj.reset_index(drop=True)
        # 添加下单日期
        dfms_jj['下单日期'] = df_ms['下单日期']
        dfms_jj = dfms_jj[['下单日期', "芝麻租物", "搜索渠道"]]
        dfms_jj.rename(columns={'芝麻租物': '芝麻租物免审进件', '搜索渠道': '搜索渠道免审进件'}, inplace=True)

        # 数据整合
        df_ms_new = dfms_jj.copy()
        df_ms_new.insert(0, '月份', df_ms_new['下单日期'].astype(str).str.split('-').str[0] + '-' +
                        df_ms_new['下单日期'].astype(str).str.split('-').str[1])
        df_ms_new.insert(2, '总体进件', df_all['进件数'])
        df_ms_new.insert(3, '芝麻租物进件', df_zm['进件数'])
        df_ms_new.insert(4, '免审进件', df_ms['进件数'])
        df_ms_new.loc[:, '免审进件占比'] = df_ms_new['免审进件'] / df_ms_new['总体进件']
        df_ms_new.loc[:, '免审进件占比'] = df_ms_new['免审进件占比'].apply(lambda x: format(x, '.2%'))
        df_ms_new.loc[:, '芝麻租物免审进件占比'] = df_ms_new['芝麻租物免审进件'] / df_ms_new['芝麻租物进件']
        df_ms_new.loc[:, '芝麻租物免审进件占比'] = df_ms_new['芝麻租物免审进件占比'].apply(lambda x: format(x, '.2%'))
        df_ms_new.loc[:, '搜索免审进件占比'] = df_ms_new['搜索渠道免审进件'] / df_ss['进件数']
        df_ms_new.loc[:, '搜索免审进件占比'] = df_ms_new['搜索免审进件占比'].apply(lambda x: format(x, '.2%'))
        df_ms_new.loc[:, '总体出库'] = df_all['出库']
        df_ms_new.loc[:, '芝麻租物出库'] = df_zm['出库']
        df_ms_new.loc[:, '免审出库'] = df_ms['出库']
        df_ms_new2 = pd.concat([df_ms_new, dfdfck_ck], axis=1)
        df_ms_new2.loc[:, '免审转化率'] = df_ms_new2['免审出库'] / df_ms_new2['免审进件']
        df_ms_new2.loc[:, '免审转化率'] = df_ms_new2['免审转化率'].apply(lambda x: format(x, '.2%'))
        df_ms_new2.loc[:, '免审出库占比'] = df_ms_new2['免审出库'] / df_ms_new2['总体出库']
        df_ms_new2.loc[:, '免审出库占比'] = df_ms_new2['免审出库占比'].apply(lambda x: format(x, '.2%'))
        df_ms_new2.loc[:, '芝麻租物免审出库占比'] = df_ms_new2['芝麻租物免审出库'] / df_ms_new2['芝麻租物出库']
        df_ms_new2.loc[:, '芝麻租物免审出库占比'] = df_ms_new2['芝麻租物免审出库占比'].apply(lambda x: format(x, '.2%'))
        df_ms_new2.loc[:, '搜索免审出库占比'] = df_ms_new2['搜索免审出库'] / df_ss['出库']
        df_ms_new2.loc[:, '搜索免审出库占比'] = df_ms_new2['搜索免审出库占比'].apply(lambda x: format(x, '.2%'))
        df_ms_new2.loc[:, '机审通过'] = df_all['机审通过件']
        df_ms_new2.loc[:, '非免审进件'] = df_ms_new2['机审通过'] - df_ms_new2['免审进件']
        df_ms_new2.loc[:, '非免审出库'] = df_ms_new2['总体出库'] - df_ms_new2['免审出库']
        df_ms_new2.loc[:, '非免审转化率'] = df_ms_new2['非免审出库'] / df_ms_new2['非免审进件']
        df_ms_new2.loc[:, '非免审转化率'] = df_ms_new2['非免审转化率'].apply(lambda x: format(x, '.2%'))
        df_ms_new3 = pd.concat([df_ms_new2, df_ms], axis=1)

        return df_ms_new3

    # 总体租完即送占比
    def rented_all(self, df_j, dfck):
        # 总体 "租完即送"占比
        df_rent = pd.crosstab(df_j["下单日期"], df_j["租赁方案"], margins=True)
        df_rent = df_rent.rename(columns={"租完即送": "租完即送进件", "租完归还": "租完归还进件", "All": "合计进件", })
        df_rent_ck = pd.crosstab(dfck["下单日期"], dfck["租赁方案"], margins=True)
        df_rent_ck = df_rent_ck.rename(
            columns={"租完即送": "租完即送出库", "租完归还": "租完归还出库", "All": "合计出库", })
        df_r = pd.merge(df_rent, df_rent_ck, left_index=True, right_index=True)
        df_r["租完即送进件占比"] = df_r["租完即送进件"] / df_r["合计进件"]
        df_r["租完即送进件占比"] = df_r["租完即送进件占比"].apply(lambda x: format(x, ".2%"))
        df_r["租完归还进件占比"] = df_r["租完归还进件"] / df_r["合计进件"]
        df_r["租完归还进件占比"] = df_r["租完归还进件占比"].apply(lambda x: format(x, ".2%"))
        df_r["租完即送出库占比"] = df_r["租完即送出库"] / df_r["合计出库"]
        df_r["租完即送出库占比"] = df_r["租完即送出库占比"].apply(lambda x: format(x, ".2%"))
        df_r["租完归还出库占比"] = df_r["租完归还出库"] / df_r["合计出库"]
        df_r["租完归还出库占比"] = df_r["租完归还出库占比"].apply(lambda x: format(x, ".2%"))
        df_r["租完即送转化率"] = df_r["租完即送出库"] / df_r["租完即送进件"]
        df_r["租完即送转化率"] = df_r["租完即送转化率"].apply(lambda x: format(x, ".2%"))
        df_r["租完归还转化率"] = df_r["租完归还出库"] / df_r["租完归还进件"]
        df_r["租完归还转化率"] = df_r["租完归还转化率"].apply(lambda x: format(x, ".2%"))

        df_r = df_r[["租完即送进件", "租完归还进件", "租完归还进件占比", "租完即送进件占比", "租完即送出库", "租完归还出库",
                "租完归还出库占比", "租完即送出库占比", '租完即送转化率', '租完归还转化率']]
        df_r_new = df_r.reset_index()
        df_r_new.loc[:, '下单日期'] = pd.to_datetime(df_r_new.下单日期[:-1]).dt.strftime('%Y-%m-%d')

        return df_r_new

    # 总体出库订单碎屏险购买数据
    def broken_screen_all(self, dfck):
        # 总体出库订单碎屏险购买数据
        df_bx_ck = pd.crosstab(dfck["下单日期"], dfck["buy_service_product"], margins=True)
        df_bx_ck = df_bx_ck.rename(columns={1: "不购买碎屏险出库", 2: "购买碎屏险出库", "All": "合计出库", })
        # '服务订单状态：1、待支付；2、待确认；3、服务中；4、已失效；5、已取消；6、已退款',
        dfck['已取消'] = np.where(dfck["service_status"] == 5, 1, 0)
        dfck['已退款'] = np.where(dfck["service_status"] == 6, 1, 0)
        dfck['待支付'] = np.where(dfck["service_status"] == 1, 1, 0)
        dfck['待确认'] = np.where(dfck["service_status"] == 2, 1, 0)
        dfck['服务中'] = np.where(dfck["service_status"] == 3, 1, 0)

        df_s3 = dfck.groupby(["下单日期"]).agg(
            {'已取消': 'sum', '已退款': 'sum', '待支付': 'sum', '待确认': 'sum', '服务中': 'sum'})

        df_s2merge = pd.merge(df_bx_ck, df_s3, left_index=True, right_index=True)
        df_s2merge["最终实际支付碎屏险出库"] = df_s2merge["待确认"] + df_s2merge["服务中"] + df_s2merge["待支付"]
        df_s2merge["实际支付碎屏险比例"] = df_s2merge["最终实际支付碎屏险出库"] / df_s2merge["合计出库"]
        df_s2merge["实际支付碎屏险比例"] = df_s2merge["实际支付碎屏险比例"].apply(lambda x: format(x, ".2%"))

        df_s2merge = df_s2merge[['不购买碎屏险出库', '购买碎屏险出库', '合计出库', '已取消', '已退款', '待支付', '待确认', '服务中',
                '最终实际支付碎屏险出库', '实际支付碎屏险比例']]
        # 芝麻租物出库订单碎屏险购买数据
        dfckzw = dfck[dfck["归属渠道"] == "芝麻租物"]
        df_zw3 = pd.crosstab(dfckzw["下单日期"], dfckzw["buy_service_product"], margins=True)
        df_zw3 = df_zw3.rename(columns={1: "不购买碎屏险出库", 2: "购买碎屏险出库", "All": "合计出库"})

        dfckzw['已取消'] = np.where(dfckzw["service_status"] == 5, 1, 0)
        dfckzw['已退款'] = np.where(dfckzw["service_status"] == 6, 1, 0)
        dfckzw['待支付'] = np.where(dfckzw["service_status"] == 1, 1, 0)
        dfckzw['待确认'] = np.where(dfckzw["service_status"] == 2, 1, 0)
        dfckzw['服务中'] = np.where(dfckzw["service_status"] == 3, 1, 0)

        df_zws3 = dfckzw.groupby(["下单日期"]).agg({'已取消': 'sum', '已退款': 'sum', '待支付': 'sum', '待确认': 'sum', '服务中': 'sum'})

        df_zws2merge = pd.merge(df_zw3, df_zws3, left_index=True, right_index=True)
        df_zws2merge["最终实际支付碎屏险出库"] = df_zws2merge["待确认"] + df_zws2merge["服务中"] + df_zws2merge[
            "待支付"]
        df_zws2merge["实际支付碎屏险比例"] = df_zws2merge["最终实际支付碎屏险出库"] / df_zws2merge["合计出库"]
        df_zws2merge["实际支付碎屏险比例"] = df_zws2merge["实际支付碎屏险比例"].apply(lambda x: format(x, ".2%"))

        df_zws2merge = df_zws2merge[
            ['不购买碎屏险出库', '购买碎屏险出库', '合计出库', '已取消', '已退款', '待支付', '待确认', '服务中',
                    '最终实际支付碎屏险出库', '实际支付碎屏险比例']]
        df_zws2merge_new = df_zws2merge.reset_index()

        # 数据整合
        df_s2merge_new = df_s2merge.reset_index()
        df_s2merge_new.loc[:, '碎屏险实际出库占比'] = (
                    df_s2merge_new.最终实际支付碎屏险出库 / df_s2merge_new.购买碎屏险出库).map(
            lambda x: format(x, ".2%"))
        df_s2merge_new.loc[:, '非芝麻实际支付碎屏险比例'] = (
                    (df_s2merge_new.购买碎屏险出库 - df_zws2merge_new.购买碎屏险出库) / (
                        df_s2merge_new.合计出库 - df_zws2merge_new.合计出库)).map(lambda x: format(x, ".2%"))
        df_s2merge_all_new = pd.concat([df_s2merge_new, df_zws2merge_new], axis=1)

        return df_s2merge_all_new

    # 商家转化数据
    def merchant(self, df_contain):
        # 定义商家的节点状态
        df_contain["待审核"] = np.where(df_contain["审核状态"] == '待审核', 1, 0)
        df_contain["前置拦截"] = np.where(df_contain["审核状态"] == '前置拦截', 1, 0)
        df_contain["机审强拒"] = np.where(df_contain["审核状态"] == '机审强拒', 1, 0)
        df_contain["人审拒绝"] = np.where(df_contain["审核状态"] == '人审拒绝', 1, 0)
        df_contain["客户取消"] = np.where(df_contain["审核状态"] == '客户取消', 1, 0)
        df_contain["出库前风控强拒"] = np.where(df_contain["审核状态"] == '出库前风控强拒', 1, 0)
        df_contain["无法联系"] = np.where(df_contain["审核状态"] == '无法联系', 1, 0)
        df_contain["是否进件"] = np.where(df_contain["进件"] == '进件', 1, 0)
        df_contain["是否出库"] = np.where(df_contain["status"].isin([2, 3, 4, 5, 6, 8, 15]), 1, 0)
        df_contain["进件前取消"] = np.where(df_contain["审核状态"] == '进件前取消', 1, 0)

        # 澄心优租
        cxyz = self.all_models.merchant_names(df_contain, '澄心优租', '下单日期')[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审通过件", '人审拒绝', '人审拒绝率',
                '待审核', "出库", "进件出库率", "订单出库率"]].reset_index()
        # 北京海鸟窝科技有限公司
        hnw = self.all_models.merchant_names(df_contain, '北京海鸟窝科技有限公司', '下单日期')[["去重订单数", "进件数", "预授权通过率", "出库", "进件出库率"]].reset_index()
        # 租着用电脑数码
        zzy = self.all_models.merchant_names(df_contain, '租着用电脑数码', '下单日期')[["去重订单数", "进件数", "预授权通过率", "出库", "进件出库率"]].reset_index()
        # 趣智数码
        qzsm = self.all_models.merchant_names(df_contain, '趣智数码', '下单日期')[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审通过件", "客户取消", '人审拒绝',
                        '人审拒绝率', '待审核', "出库", "进件出库率", "取消率", "订单出库率"]].reset_index()
        # 汇客好租
        hkhz = self.all_models.merchant_names(df_contain, '汇客好租', '下单日期')[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审通过件", "客户取消", '人审拒绝',
                        '人审拒绝率', '待审核', "出库", "进件出库率", "取消率", "订单出库率"]].reset_index()
        # 小蚂蚁租机
        xmy = self.all_models.merchant_names(df_contain, '小蚂蚁租机', '下单日期')[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审通过件", "客户取消", '人审拒绝',
                    '人审拒绝率', '待审核', "出库", "进件出库率", "取消率", "订单出库率"]].reset_index()
        # 乙辉数码
        yhsm = self.all_models.merchant_names(df_contain, '乙辉数码', '下单日期')[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审通过件", "客户取消", '人审拒绝',
                        '人审拒绝率', '待审核', "出库", "进件出库率", "取消率", "订单出库率"]].reset_index()
        # 兴鑫兴通讯
        xxx = self.all_models.merchant_names(df_contain, '兴鑫兴通讯', '下单日期')[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审通过件", "客户取消", '人审拒绝',
                    '人审拒绝率', '待审核', "出库", "进件出库率", "取消率", "订单出库率"]].reset_index()

        return cxyz, hnw, zzy, qzsm, hkhz, xmy, yhsm, xxx

    # 各渠道进件出库统计
    def statistics(self, df, df_j, dfck):
        # 每日各渠道去重订单统计
        gsqd_qcdd = pd.crosstab(df["下单日期"], df["归属渠道"], margins=True)
        lyqd_qcdd = pd.crosstab(df["下单日期"], df["来源渠道"], margins=True)
        hdmc_qcdd = pd.crosstab(df["下单日期"], df["activity_name"], margins=True)
        # 每日各渠道进件统计
        gsqd_jj = pd.crosstab(df_j["下单日期"], df_j["归属渠道"], margins=True)
        lyqd_jj = pd.crosstab(df_j["下单日期"], df_j["来源渠道"], margins=True)
        hdmc_jj = pd.crosstab(df_j["下单日期"], df_j["activity_name"], margins=True)
        # 每日各渠道出库统计
        gsqd_ck = pd.crosstab(dfck["下单日期"], dfck["归属渠道"], margins=True)
        lyqd_ck = pd.crosstab(dfck["下单日期"], dfck["来源渠道"], margins=True)
        hdmc_ck = pd.crosstab(dfck["下单日期"], dfck["activity_name"], margins=True)

        return gsqd_qcdd, lyqd_qcdd, hdmc_qcdd, gsqd_jj, lyqd_jj, hdmc_jj, gsqd_ck, lyqd_ck, hdmc_ck

    # 获取拒量数据
    def rejected(self, df):
        # 设置时间段
        now_day = datetime.now()
        seven_ago = now_day - pd.Timedelta(days=8)
        one_ago = now_day - pd.Timedelta(days=1)
        # 获取特定时间段的拒量订单的转化数据和明细
        df_jl = df[(df.下单日期 >= seven_ago) & (df.下单日期 <= one_ago) & (df.tips.str.contains('策略241205'))]
        df_jl_new = df_jl[~df_jl.tips.str.contains('策略241205命中(1)', regex=False)]
        df_jl_new['策略命中等级'] = df_jl_new['tips'].str.extract(r'(策略241205命中\(\d+\))')[0]
        # 进件数，出库数，出库率，风险等级
        df_jl_new_group = df_jl_new.groupby('下单日期').agg({'是否进件': 'sum', '是否出库': 'sum'})
        df_jl_new_group.rename(columns={'是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        df_jl_new_group['进件出库率'] = (df_jl_new_group.出库 / df_jl_new_group.进件数).map(lambda x: format(x, '.2%'))
        df_jl_new2 = df_jl_new[['下单日期', 'order_number', '策略命中等级']]
        df_jl_new2_group = df_jl_new2.groupby('策略命中等级').agg(数量=('order_number', 'count'))

        return df_jl_new_group, df_jl_new2_group


    # 修改日报名称
    def update_report(self):
        # 获取当前日期和前1天、2天以及4天的日期
        now = datetime.now()
        now_day = pd.to_datetime(now.strftime("%Y-%m-%d"))
        now_date = now.strftime('%m%d')
        before_date = (now - timedelta(days=1)).strftime('%m%d')
        before_date_two = (now - timedelta(days=2)).strftime('%m%d')
        before_date_four = (now - timedelta(days=4)).strftime('%m%d')
        monday = pd.to_datetime(now.strftime('%Y-%m-%d')).day_name()
        # 指定目录路径
        directory = 'F:/日报/自动化日报'
        # 查找所有 .xlsx 和 .xls 文件
        excel_files = glob.glob(os.path.join(directory, '*.xlsx')) + glob.glob(os.path.join(directory, '*.xls'))
        for old_file_path in excel_files:
            # 提取文件名（不带路径）
            old_filename = os.path.basename(old_file_path)
            if monday == 'Monday':
                # 构造新的文件名（例如添加前缀或后缀）
                new_filename = old_filename.replace(before_date_four, before_date)
            else:
                # 构造新的文件名（例如添加前缀或后缀）
                new_filename = old_filename.replace(before_date_two, before_date)
            # 将新的文件名添加到路径
            new_file_path = os.path.join(directory, new_filename)
            # 确保新文件名不存在，以避免覆盖文件
            if not os.path.exists(new_file_path):
                os.rename(old_file_path, new_file_path)
                print(f"文件 {old_filename} 已重命名为: {new_filename}")
            else:
                print(f"跳过文件 {old_filename}，因为新文件名已存在。")

        # 中文星期映射 获取星期几
        day_name_cn_mapping = {
            'Monday': '星期一',
            'Tuesday': '星期二',
            'Wednesday': '星期三',
            'Thursday': '星期四',
            'Friday': '星期五',
            'Saturday': '星期六',
            'Sunday': '星期日'
        }
        # 设置需要打开的文件和密码
        file_path1 = f'F:/日报/自动化日报/迪瓜租机订单日报截止{before_date}.xlsx'
        file_path2 = f'F:/日报/自动化日报/商户订单统计表截至{before_date}.xlsx'
        password = '20240101'
        return day_name_cn_mapping, file_path1, file_path2, password, now_day

    def run(self, hour, minute):
        print('正在查询数据...')
        df_order, df_risk_examine, df_ck = self.select_data()
        print('数据查询完毕...\n正在清理数据...')
        df_contain, df, df2, dfck, df_j = self.clean_data(df_order,df_ck)
        print('数据清理完毕...\n正在获取数据...')
        df_all2, df_ss_group2, df_zm_group2 = self.get_data_hour(df, df2, df_risk_examine, hour, minute)
        print('数据获取完毕...')
        return df_all2, df_ss_group2, df_zm_group2
        # print('数据清理完毕...\n正在获取出库订单数据...')
        # df_weekday_zh, df_ly_ck, df_zfb_ck, df_my = self.order_ck(dfck)
        # print('出库单数获取完毕...\n正在获取渠道数据...')
        # self.df_all, self.df_ss, self.df_dr, self.df_zm, self.df_zw, self.df_dy, self.df_zfb, self.df_ms, self.df_ms_order, self.df_qmy, self.df_fmy = self.channel(df, df2, df_risk_examine, df_weekday_zh, df_zfb_ck, df_my, '下单日期')
        # print('渠道数据获取完毕...\n正在获取总体剔除直播后的数据...')
        # self.df_tc = self.all_tc(self.df_all, self.df_dr, self.df_dy, self.df_zfb)
        # print('总体剔除直播后的数据获取完毕...\n正在进行免审数据整合...')
        # self.df_ms_new3 = self.data_integration(df, dfck, self.df_all, self.df_ss, self.df_zm, self.df_ms)
        # print('免审数据整合完毕...\n正在获取总体租完即送数据...')
        # self.df_r_new = self.rented_all(df_j, dfck)[:-1]
        # print('总体租完即送数据获取完毕...\n正在获取总体出库订单碎屏险购买数据...')
        # self.df_s2merge_all_new = self.broken_screen_all(dfck)
        # print('总体出库订单碎屏险购买数据获取完毕...\n正在获取商家转化数据...')
        # self.cxyz, self.hnw, self.zzy, self.qzsm, self.hkhz, self.xmy, self.yhsm, self.xxx = self.merchant(df_contain)
        # print('商家转化数据获取完毕...\n正在获取各渠道进件统计数据...')
        # self.gsqd_qcdd, self.lyqd_qcdd, self.hdmc_qcdd, self.gsqd_jj, self.lyqd_jj, self.hdmc_jj, self.gsqd_ck, self.lyqd_ck, self.hdmc_ck = self.statistics(df, df_j, dfck)
        # print('各渠道进件统计数据获取完毕...\n正在获取拒量数据...')
        # self.df_jl_new_group, self.df_jl_new2_group = self.rejected(df)
        # print('拒量数据获取完毕...')

    # 创建定时任务
    def my_job(self, hour, minute, path, path1, path2, hour_date, minute_date):
        print(f'执行定时任务：现在是每日的{hour}:{minute}')
        df_all2, df_ss_group2, df_zm_group2 = self.run(hour_date, minute_date)
        # 写入日报相关数据
        # with pd.ExcelWriter(path1 + f'日报相关数据输出_{self.Today}.xlsx',engine='openpyxl') as writer:
        #     self.gsqd_qcdd.to_excel(writer, sheet_name='归属渠道去重订单统计', index=True)
        #     self.lyqd_qcdd.to_excel(writer, sheet_name='来源渠道去重订单统计', index=True)
        #     self.hdmc_qcdd.to_excel(writer, sheet_name='活动名称去重订单统计', index=True)
        #     self.gsqd_jj.to_excel(writer, sheet_name='归属渠道进件统计', index=True)
        #     self.lyqd_jj.to_excel(writer, sheet_name='来源渠道进件统计', index=True)
        #     self.hdmc_jj.to_excel(writer, sheet_name='活动名称进件统计', index=True)
        #     self.gsqd_ck.to_excel(writer, sheet_name='归属渠道出库统计', index=True)
        #     self.lyqd_ck.to_excel(writer, sheet_name='来源渠道出库统计', index=True)
        #     self.hdmc_ck.to_excel(writer, sheet_name='活动名称出库统计', index=True)
        with pd.ExcelWriter(path + f'渠道转化_{self.Today}.xlsx', engine='xlsxwriter') as writer:
            df_all2.to_excel(writer, sheet_name='总体')
            df_ss_group2.to_excel(writer, sheet_name='搜索渠道')
            df_zm_group2.to_excel(writer, sheet_name='芝麻租物') # [-7:]

        # 写入拒量数据
        # 获取当前是星期几
        # Monday = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')).day_name()
        # if Monday == 'Monday':
        #     with pd.ExcelWriter(path2 + f'拒量数据_{self.Today}.xlsx', engine='openpyxl') as writer:
        #         self.df_jl_new2_group.to_excel(writer, sheet_name='转化数据')
        #         self.df_jl_new2_group.to_excel(writer, sheet_name='拒量数据明细')
        #
        # # 自动写入日报数据
        # day_name_cn_mapping, file_path1, file_path2, password, now_day = self.update_report()
        # sheet_name = '2025年01月'
        # #  总体
        # self.df_all.insert(0, '星期', self.df_all['下单日期'].apply(lambda x: day_name_cn_mapping[x.day_name()]))
        # self.df_all.insert(1, '月份', self.df_all['下单日期'].astype(str).str.split('-').str[0] + '-' +
        #                     self.df_all['下单日期'].astype(str).str.split('-').str[1])
        # col_len = self.all_models.Open_Excel(self.df_all[:-1], file_path1, password, sheet_name)
        # # 搜索渠道
        # col_len1 = self.all_models.Open_Excel(self.df_ss[:-1], file_path1, password, sheet_name, col_len)
        # # 单人会话
        # col_len2 = self.all_models.Open_Excel(self.df_dr[:-1], file_path1, password, sheet_name, col_len1)
        # # 芝麻租物
        # col_len3 = self.all_models.Open_Excel(self.df_zm[:-1], file_path1, password, sheet_name, col_len2)
        # # 纯租物
        # col_len4 = self.all_models.Open_Excel(self.df_zw[:-1], file_path1, password, sheet_name, col_len3)
        # # 总体剔除直播
        # col_len5 = self.all_models.Open_Excel(self.df_tc[:-1], file_path1, password, sheet_name, col_len4)
        # # 抖音渠道
        # if len(self.df_dy) == 16:
        #     col_len6 = self.all_models.Open_Excel(self.df_dy[:-1], file_path1, password, sheet_name, col_len5, '抖音')
        # else:
        #     col_len6 = self.all_models.Open_Excel(self.df_dy, file_path1, password, sheet_name, col_len5, '抖音')
        # #  支付宝直播
        # if len(self.df_zfb) == 16:
        #     col_len7 = self.all_models.Open_Excel(self.df_zfb[:-1], file_path1, password, sheet_name, col_len6)
        # else:
        #     col_len7 = self.all_models.Open_Excel(self.df_zfb, file_path1, password, sheet_name, col_len6)
        # ## 免人审数据
        # self.all_models.Open_Excel(self.df_ms_new3[:-1], file_path1, password, '免人审数据', key=1)
        # ## 免审订单转化统计
        # self.all_models.Open_Excel(self.df_ms_order[:-1], file_path1, password, '免审订单转化统计', key=1)
        # # 全免押
        # yj_col_len = self.all_models.Open_Excel(self.df_qmy[:-1], file_path1, password, '押金类型', key='押金')
        # # # 非免押
        # self.all_models.Open_Excel(self.df_fmy[:-1], file_path1, password, '押金类型', col_len=yj_col_len, key='押金')
        # ## 租完即送占比
        # df_r_new_max_date = pd.to_datetime(self.df_r_new.下单日期, format='%Y-%m-%d').max()
        # if df_r_new_max_date<now_day:
        #     self.df_r_new.loc[len(self.df_r_new)] = np.nan
        #     self.all_models.Open_Excel(self.df_r_new[:-1], file_path1, password, '租完即送占比')
        # else:
        #     self.all_models.Open_Excel(self.df_r_new[:-1], file_path1, password, '租完即送占比')
        # ## 碎屏险数据
        # df_s2merge_all_new_max_date = self.df_s2merge_all_new.iloc[:, 0].max()
        # self.all_models.Open_Excel(self.df_s2merge_all_new, file_path1, password, '碎屏险数据')
        #
        # # 商家数据
        # ### 澄心优租
        # cxyz_new_max_date = self.cxyz.下单日期.max()
        # if cxyz_new_max_date < now_day:
        #     self.cxyz.loc[len(self.cxyz)] = np.nan
        #     self.all_models.Open_Excel(df=self.cxyz, path=file_path2, password=password, sheet_name='澄心优租')
        # else:
        #     self.all_models.Open_Excel(df=self.cxyz, path=file_path2, password=password, sheet_name='澄心优租')
        # ### 北京海鸟窝科技有限公司
        # hnw_new_max_date = self.hnw.下单日期.max()
        # if hnw_new_max_date < now_day:
        #     self.hnw.loc[len(self.hnw)] = np.nan
        #     self.all_models.Open_Excel(df=self.hnw, path=file_path2, password=password, sheet_name='海鸟窝')
        # else:
        #     self.all_models.Open_Excel(df=self.hnw, path=file_path2, password=password, sheet_name='海鸟窝')
        # ### 租着用
        # zzy_new_max_date = self.zzy.下单日期.max()
        # if zzy_new_max_date < now_day:
        #     self.zzy.loc[len(self.zzy)] = np.nan
        #     self.all_models.Open_Excel(df=self.zzy, path=file_path2, password=password, sheet_name='租着用')
        # else:
        #     self.all_models.Open_Excel(df=self.zzy, path=file_path2, password=password, sheet_name='租着用')
        # ### 趣智数码
        # qzsm_new_max_date = self.qzsm.下单日期.max()
        # if qzsm_new_max_date < now_day:
        #     self.qzsm.loc[len(self.qzsm)] = np.nan
        #     self.all_models.Open_Excel(df=self.qzsm, path=file_path2, password=password, sheet_name='趣智数码')
        # else:
        #     self.all_models.Open_Excel(df=self.qzsm, path=file_path2, password=password, sheet_name='趣智数码')
        # ### 汇客好租
        # hkhz_new_max_date = self.hkhz.下单日期.max()
        # if hkhz_new_max_date < now_day:
        #     self.hkhz.loc[len(self.hkhz)] = np.nan
        #     self.all_models.Open_Excel(df=self.hkhz, path=file_path2, password=password, sheet_name='汇客好租')
        # else:
        #     self.all_models.Open_Excel(df=self.hkhz, path=file_path2, password=password, sheet_name='汇客好租')
        # ### 小蚂蚁租机
        # xmy_new_max_date = self.xmy.下单日期.max()
        # if xmy_new_max_date < now_day:
        #     self.xmy.loc[len(self.xmy)] = np.nan
        #     self.all_models.Open_Excel(df=self.xmy, path=file_path2, password=password, sheet_name='小蚂蚁租机')
        # else:
        #     self.all_models.Open_Excel(df=self.xmy, path=file_path2, password=password, sheet_name='小蚂蚁租机')
        # ### 乙辉数码
        # yhsm_new_max_date = self.yhsm.下单日期.max()
        # if yhsm_new_max_date < now_day:
        #     self.yhsm.loc[len(self.yhsm)] = np.nan
        #     self.all_models.Open_Excel(df=self.yhsm, path=file_path2, password=password, sheet_name='乙辉数码')
        # else:
        #     self.all_models.Open_Excel(df=self.yhsm, path=file_path2, password=password, sheet_name='乙辉数码')

        ### 兴鑫兴通讯
        # xxx_new_max_date = self.xxx.下单日期.max()
        # if xxx_new_max_date < now_day:
        #     self.xxx.loc[len(self.xxx)] = np.nan
        #     self.all_models.Open_Excel(df=self.xxx, path=file_path2, password=password, sheet_name='兴鑫兴通讯')
        # else:
        #     self.all_models.Open_Excel(df=self.xxx, path=file_path2, password=password, sheet_name='兴鑫兴通讯')


if __name__ == '__main__':
    hour = 18
    minute = 1
    path = r'\\digua\迪瓜租机\19.小程序发货率/'
    path1 = r'\\digua\迪瓜租机\20.日报数据相关输出/'
    path2 = r'\\digua\迪瓜租机\22.拒量数据/'
    rd = Report_Day()
    # rd.run()
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # job = scheduler.add_job(rd.my_job, 'cron', day_of_week='mon-fri', hour=hour, minute=minute, args=[hour, minute, path1, path2])
    job = scheduler.add_job(rd.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path, path1, path2, 18, '00'])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
