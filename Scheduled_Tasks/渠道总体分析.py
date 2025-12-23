from time import sleep

import numpy as np
import pandas as pd
import xlwings as xw
from openpyxl import load_workbook
import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
from apscheduler.schedulers.background import BackgroundScheduler
import gc
import warnings
warnings.filterwarnings("ignore")
import requests
import hmac
import hashlib
import base64
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

class Channel:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        # 实例化All_Model类
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()
        # 替换为你的 Webhook 地址
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=85b55eb8850e4506c0145c0249d7465cf787a4332e9829c26d3db55329bac3a7"
        # 替换为你的密钥，如果没有设置则留空
        self.secret = "SECc6b1496b6ba837fa20ef46b93d7788c561c4b8a9dec206f838642f3b6fafd78a"


    # 查询数据
    def select_data(self, hour):
        sql1 = f''' -- 订单&风控信息  近10日数据   
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
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.queue_verify_type') end,'"','') as qvt_risk
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.queue_verify_type') end,'"','') as qvt_result 
        ,cc.name as channel_name         -- 来源渠道
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_id,om.merchant_name
        ,topay.total_freeze_fund_amount , tod.dy_order_item_json, pa.type
        ,om.buy_service_product,tso.status as service_status 
        ,om.order_method, om.activity_id, om.appid, tprm.max_overdue_days
        , tor.update_time, tomt.reason, cc.channel_type_id, om.order_type, om.union_rent_tag, tojo.app_type
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
        -- 京东外部订单关联表
        left join db_digua_business.t_order_jd_out_no tojo on tojo.order_id=om.id
        where om.user_mobile is not null 
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        -- and pa.type!=4
        and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -15 day )
--         and DATE_FORMAT(om.create_time,'%Y-%m-%d')>='2025-06-09'
--         and  DATE_FORMAT(om.create_time, '%Y-%m-%d')<='2025-07-05'
        and hour(om.create_time)<'{hour}'
        ;
        '''
        sql3 = ''' -- 拒量拒绝原因
        SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status  
        FROM `db_credit`.risk_examine
        '''

        sql_risk = ''' -- risk等级
        select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r 

        -- , replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.union_rent_tag') end,'"','') as union_rent_tag
        -- , replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.union_rent_rejected') end,'"','') as union_rent_rejected 
        from db_credit.risk
        '''
        df_risk = self.clean.query(sql_risk)

        sql_ra = ''' -- 996强拒表
        select order_id, time, replace(case when JSON_VALID(result) THEN JSON_EXTRACT(result, '$.status') end,'"','') as status_ra  
        from db_credit.risk_alipay_interactive_prod_result
        '''
        df_ra = self.clean.query(sql_ra)
        df_ra.loc[:, 'time_ra'] = pd.to_datetime(df_ra.time.dt.date, errors="coerce")
        df_ra = df_ra.sort_values(by='time', ascending=False).groupby('order_id').head(1)
        # sql_ck = ''' -- 台账出库数据
        # select date, order_number, category, remark from db_digua_business.t_ledger
        # '''
        f_path_ck = "F:/myfile/p站数据/台账数据/维客壹佰2025年台账.xlsx"
        df_ck = pd.read_excel(f_path_ck, sheet_name="2025")
        df_order = self.clean.query(sql1)
        df_order = df_order[df_order.type != 4]
        df_risk_examine = self.clean.query(sql3)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)
        return df_order, df_risk_examine, df_ck, df_risk, df_re, df_ra

    # 数据处理
    def clean_data(self, df, df_ck, df_risk, df_re, df_ra):
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
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[
            0].str.strip()
        df.loc[:, "商品类型"] = np.where(
            df["product_name"].str.contains('99新') | df["product_name"].str.contains('95新') | df[
                "product_name"].str.contains('准新'), "二手", "全新")  ##  准新
        df.loc[:, "租赁方案"] = np.where(df["sku_attributes"].str.contains('租完即送'), "租完即送", "租完归还")
        df.loc[:, "押金类型"] = np.where(df["total_freeze_fund_amount"] > 0, "部分免押", "全免押")
        df.loc[:, "优惠券使用否"] = np.where((df["new_actual_money"] - df["all_money"] > 0) & (df["租赁方案"] == '租完即送'), "已使用", "未使用")
        df.loc[:, "是否进行预授权"] = np.where(df["total_freeze_fund_amount"].isnull(), "未预授权", "已预授权")

        df.loc[:, "颜色"] = df.apply(lambda x: self.clean.getcolor(x["sku_attributes"]), axis=1)
        df.loc[:, "内存"] = df.apply(lambda x: self.clean.getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'], x['order_type']), axis=1)
        # 保留去重前的数据
        df_no_drop = df.copy()
        # 订单去重
        df = self.clean.order_drop_duplicates(df)
        # 删除重复订单
        df.drop_duplicates(subset=["order_id"], inplace=True)


        # 定义状态 , 'union_rent_tag', 'union_rent_rejected'
        df = df.merge(df_risk[['trace_id', 'status_r']], on='trace_id', how='left').merge(
            df_re[['trace_id', 'status_re']], on='trace_id', how='left').merge(
            df_ra[['order_id', 'time_ra', 'status_ra']], left_on=['order_id', '下单日期'],
            right_on=['order_id', 'time_ra'], how='left')

        # 判断 前置拦截   机审强拒   出库前风控强拒
        df['是否前置拦截'] = np.where(
            (df.result.str.contains('id_card不得为空')) | (df.result.str.contains('mobile校验不通过')) | (
                df.result.str.contains('name校验不通过'))
            | (df.result.str.contains('年龄超过49岁或低于18岁')) | (df.result == '风控未通过') | (
                    df.result == '黑名单用户'), 1, 0)
        df['是否机审强拒'] = np.where(df.status_r == '1', 1, 0)
        df['是否出库前风控强拒'] = np.where((df.status_re == '1') | (df.status_ra == '1'), 1, 0)
        df.loc[:, "审核状态"] = df.apply(
            lambda x: self.clean.reject_type(x["拒绝理由"], x["进件"], x["电审拒绝原因"], x["取消原因"],
                                             x["status2"], x["无法联系原因"], x["total_describes"],
                                             x['是否前置拦截'], x['是否机审强拒'], x['是否出库前风控强拒']), axis=1)
        df['取消原因2'] = df['cancel_reason'].str.split('：')
        df['取消原因2'] = df['取消原因2'].apply(lambda x: x[-1] if x is not None else x)
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
        # 取到没有去重的数据
        df_ll = df.copy()
        # 排序去重
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)
        # 关联台账数据
        dfck = pd.merge(df_ck, df, left_on="订单号", right_on='order_number')
        dfck.drop_duplicates(subset=["order_number"], inplace=True)
        # 删除已退款订单
        dfck.drop(dfck[dfck["status2"] == "已退款"].index, inplace=True)
        # 删除 露营设备 出库
        try:
            dfck.drop(dfck[dfck["类目"] == "露营设备"].index, inplace=True)
        except:
            dfck.drop(dfck[dfck["类型"] == "露营设备"].index, inplace=True)

        df2 = df.copy()
        df2_ll = df_ll.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        df2_ll = self.clean.drop_rejected_merchant(df2_ll)
        return df_contain, df, df2, dfck, df_j, df_ll, df2_ll, df_no_drop

    # 获取数据，总体，搜索渠道，芝麻租物的转化
    def get_data_hour(self,df, df2, df_risk_examine):
        # 总体
        df_all2 = self.all_models.data_group(df, df2, df_risk_examine, ['下单日期'])
        df_all2 = df_all2[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系","出库前风控强拒","待审核",'出库', '进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']]
        # 搜索渠道
        df_ss = df[df.归属渠道=='搜索渠道']
        df_ss2 = df2[df2.归属渠道 == '搜索渠道']
        df_ss_group2 = self.all_models.data_group(df_ss, df_ss2, df_risk_examine, ['下单日期'])
        df_ss_group2 = df_ss_group2[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件","人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系", "出库前风控强拒", "待审核", '出库','进件出库率', '取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率']]
        # 芝麻租物
        df_zm = df[df.归属渠道 == '芝麻租物']
        df_zm2 = df2[df2.归属渠道 == '芝麻租物']
        df_zm_group2 = self.all_models.data_group(df_zm, df_zm2, df_risk_examine, ['下单日期'])
        df_zm_group2 = df_zm_group2[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系","出库前风控强拒","待审核",'出库','进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']]
        return df_all2, df_ss_group2, df_zm_group2

    # 全域租物
    def get_zw(self, df, df2, df_risk_examine, path, t_date):
        channel_list = ['侠客行全域租物', '侠客行租物搜索', '创本全域租物', '创本租物搜索', '邦道全域租物',
                        '邦道租物搜索']
        # 通过循环获取每个渠道的数据并写入表格
        for idx, name in enumerate(channel_list):
            df_6 = df[df.来源渠道 == name]
            df2_6 = df2[df2.来源渠道 == name]
            df_6_group = self.all_models.data_group(df_6, df2_6, df_risk_examine, '下单日期')[
                ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
                 "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
                 "出库前风控强拒", "待审核", '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例',
                 '无法联系占比', '订单出库率']]
            df_6_group_reject = df_6.groupby('拒绝理由').agg({'order_number': 'count'}).rename(
                columns={'order_number': '数量'})
            df_6_group_qx = df_6.groupby('取消原因2').agg({'order_number': 'count'}).rename(
                columns={'order_number': '数量'})
            if idx == 0:
                with pd.ExcelWriter(path + f'全域租物_{t_date}.xlsx', engine='xlsxwriter') as writer:
                    df_6_group.to_excel(writer, sheet_name=name)
                with pd.ExcelWriter(path + f'全域租物被拒原因_{t_date}.xlsx', engine='xlsxwriter') as writer:
                    df_6_group_reject.to_excel(writer, sheet_name=name)
                with pd.ExcelWriter(path + f'全域租物取消原因_{t_date}.xlsx', engine='xlsxwriter') as writer:
                    df_6_group_qx.to_excel(writer, sheet_name=name)
            else:
                with pd.ExcelWriter(path + f'全域租物_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
                    df_6_group.to_excel(writer, sheet_name=name)
                with pd.ExcelWriter(path + f'全域租物被拒原因_{t_date}.xlsx', engine='openpyxl',
                                    mode='a') as writer:
                    df_6_group_reject.to_excel(writer, sheet_name=name)
                with pd.ExcelWriter(path + f'全域租物取消原因_{t_date}.xlsx', engine='openpyxl',
                                    mode='a') as writer:
                    df_6_group_qx.to_excel(writer, sheet_name=name)

        df_6 = df[df.来源渠道.isin(channel_list[::2])]
        df2_6 = df2[df2.来源渠道.isin(channel_list[::2])]
        df_6_group = self.all_models.data_group(df_6, df2_6, df_risk_examine, '下单日期')[
            ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
             "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
             "出库前风控强拒", "待审核", '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比',
             '订单出库率']]
        df_6_group_reject = df_6.groupby('拒绝理由').agg({'order_number': 'count'}).rename(
            columns={'order_number': '数量'})
        df_6_group_qx = df_6.groupby('取消原因2').agg({'order_number': 'count'}).rename(
            columns={'order_number': '数量'})
        with pd.ExcelWriter(path + f'全域租物_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
            df_6_group.to_excel(writer, sheet_name='租物汇总')
        with pd.ExcelWriter(path + f'全域租物被拒原因_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
            df_6_group_reject.to_excel(writer, sheet_name='租物汇总')
        with pd.ExcelWriter(path + f'全域租物取消原因_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
            df_6_group_qx.to_excel(writer, sheet_name='租物汇总')

        df_6 = df[df.来源渠道.isin(channel_list[1::2])]
        df2_6 = df2[df2.来源渠道.isin(channel_list[1::2])]
        df_6_group = self.all_models.data_group(df_6, df2_6, df_risk_examine, '下单日期')[
            ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
             "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
             "出库前风控强拒", "待审核", '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比',
             '订单出库率']]
        df_6_group_reject = df_6.groupby('拒绝理由').agg({'order_number': 'count'}).rename(
            columns={'order_number': '数量'})
        df_6_group_qx = df_6.groupby('取消原因2').agg({'order_number': 'count'}).rename(
            columns={'order_number': '数量'})
        with pd.ExcelWriter(path + f'全域租物_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
            df_6_group.to_excel(writer, sheet_name='搜索汇总')
        with pd.ExcelWriter(path + f'全域租物被拒原因_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
            df_6_group_reject.to_excel(writer, sheet_name='搜索汇总')
        with pd.ExcelWriter(path + f'全域租物取消原因_{t_date}.xlsx', engine='openpyxl', mode='a') as writer:
            df_6_group_qx.to_excel(writer, sheet_name='搜索汇总')

    # 支付宝联合运营
    def get_lhyy(self, df, df2, df_risk_examine):
        today = datetime.today()
        t_date = today.date().strftime('%Y-%m-%d')
        # 获取联合运营订单的转化数据
        df_lhyy = df[df.union_rent_tag=='Y']
        df2_lhyy = df2[df2.union_rent_tag=='Y']

        df_group = self.all_models.data_group_contain_hl(df_lhyy, df2_lhyy, df_risk_examine, '下单日期')
        df_group = df_group[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
                  "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
                  "出库前风控强拒", "待审核", '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例',
                  '无法联系占比', '订单出库率']]
        # 获取要发送消息的数据
        df_group_new = df_group.iloc[-1, :]
        jj = df_group_new.loc['进件数']
        qx = df_group_new.loc['机审强拒'] + df_group_new.loc['人审拒绝'] + df_group_new.loc['客户取消'] + df_group_new.loc['无法联系'] + df_group_new.loc['出库前风控强拒']
        sh = df_group_new.loc['待审核']
        tg = df_group_new.loc['出库']
        # 各位好，今天进件5单，客户取消0单， 待审核3单，通过2单
        if jj != 0:
            info = f'''各位好，今天进件{jj}单，客户取消{int(qx)}单，待审核{sh}单，通过{tg}单'''
        else:
            info = '各位好，今天没有进件'
        return df_group, info
    # 京享租
    def jd(self, df_no_drop, df, df2, df_risk_examine):
        # 获取京享租右卡活动数据
        df_no_drop_jd = df_no_drop[df_no_drop.来源渠道 == '京享租右卡']
        df_no_drop_jd.loc[:, '创建进件数'] = np.where(df_no_drop_jd.进件 == '进件', 1, 0)
        df_no_drop_jd_g = df_no_drop_jd.groupby('下单日期').agg({'order_id': 'count', '创建进件数': 'sum'}).rename(columns={'order_id': '创建订单数'})
        df_jd_yk = df[df['来源渠道'] == '京享租右卡']
        df_jd2_yk = df2[df2['来源渠道'] == '京享租右卡']
        df_jd_group = self.all_models.data_group(df_jd_yk, df_jd2_yk, df_risk_examine, '下单日期')
        df_jd_group = df_jd_group[
            ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
             "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系", "出库前风控强拒", "待审核",
             '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例',
             '无法联系占比', '订单出库率']].fillna(0)
        df_jd_group_new = df_jd_group.reset_index()
        df_jd_group_new = df_no_drop_jd_g.merge(df_jd_group_new, on='下单日期', how='inner')
        return df_jd_group_new

    # def jd(self, df_no_drop, df, df2, df_risk_examine):
    #     try:
    #         # 初始化一个空的DataFrame作为默认返回值
    #         result_df = pd.DataFrame(
    #             columns=['下单日期', '创建订单数', '创建进件数', '去重订单数', '前置拦截', '拦截率',
    #                      '进件数', '预授权通过率', '机审强拒', '强拒比例', '机审通过件',
    #                      '人审拒绝', '风控通过件', '风控通过率', '客户取消', '无法联系',
    #                      '出库前风控强拒', '待审核', '出库', '进件出库率', '取消率',
    #                      '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率'])
    #
    #         # 获取京享租右卡活动数据
    #         df_no_drop_jd = df_no_drop[df_no_drop.来源渠道 == '京享租右卡']
    #         if df_no_drop_jd.empty:
    #             print("警告：没有找到京享租右卡的数据")
    #             return result_df
    #
    #         df_no_drop_jd.loc[:, '创建进件数'] = np.where(df_no_drop_jd.进件 == '进件', 1, 0)
    #         df_no_drop_jd_g = df_no_drop_jd.groupby('下单日期').agg({'order_id': 'count', '创建进件数': 'sum'}).rename(
    #             columns={'order_id': '创建订单数'})
    #
    #         df_jd_yk = df[df['来源渠道'] == '京享租右卡']
    #         df_jd2_yk = df2[df2['来源渠道'] == '京享租右卡']
    #
    #         if df_jd_yk.empty or df_jd2_yk.empty:
    #             print("警告：没有找到京享租右卡的进件数据")
    #             return df_no_drop_jd_g  # 返回只有创建订单数和创建进件数的数据
    #
    #         df_jd_group = self.all_models.data_group(df_jd_yk, df_jd2_yk, df_risk_examine, '下单日期')
    #         if df_jd_group.empty:
    #             print("警告：京享租右卡的数据分组结果为空")
    #             return df_no_drop_jd_g  # 返回只有创建订单数和创建进件数的数据
    #
    #         df_jd_group = df_jd_group[
    #             ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
    #              "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系", "出库前风控强拒", "待审核",
    #              '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例',
    #              '无法联系占比', '订单出库率']].fillna(0)
    #
    #         df_jd_group_new = df_jd_group.reset_index()
    #         df_jd_group_new = df_no_drop_jd_g.merge(df_jd_group_new, on='下单日期', how='inner')
    #
    #         if df_jd_group_new.empty:
    #             print("警告：合并后的京享租右卡数据为空")
    #             return df_no_drop_jd_g  # 返回只有创建订单数和创建进件数的数据
    #
    #         return df_jd_group_new
    #
    #     except Exception as e:
    #         print(f"处理京享租右卡数据时出错: {str(e)}")
    #         # 返回一个空的DataFrame，包含所有需要的列
    #         return pd.DataFrame(columns=['下单日期', '创建订单数', '创建进件数', '去重订单数', '前置拦截', '拦截率',
    #                                      '进件数', '预授权通过率', '机审强拒', '强拒比例', '机审通过件',
    #                                      '人审拒绝', '风控通过件', '风控通过率', '客户取消', '无法联系',
    #                                      '出库前风控强拒', '待审核', '出库', '进件出库率', '取消率',
    #                                      '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率'])

    def jd2(self, df_no_drop, df, df2, df_risk_examine):
        # 获取京东618活动数据
        df_no_drop_jd = df_no_drop[df_no_drop.来源渠道 == '京东618活动']
        df_no_drop_jd.loc[:, '创建进件数'] = np.where(df_no_drop_jd.进件 == '进件', 1, 0)
        df_no_drop_jd_g = df_no_drop_jd.groupby('下单日期').agg({'order_id': 'count', '创建进件数': 'sum'}).rename(columns={'order_id': '创建订单数'})
        df_jd_yk = df[df['来源渠道'] == '京东618活动']
        df_jd2_yk = df2[df2['来源渠道'] == '京东618活动']
        df_jd_group = self.all_models.data_group(df_jd_yk, df_jd2_yk, df_risk_examine, '下单日期')
        df_jd_group = df_jd_group[
            ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
             "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系", "出库前风控强拒", "待审核",
             '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例',
             '无法联系占比', '订单出库率']].fillna(0)
        df_jd_group_new = df_jd_group.reset_index()
        df_jd_group_new2 = df_no_drop_jd_g.merge(df_jd_group_new, on='下单日期', how='inner')
        return df_jd_group_new2



    def jd3(self, df_no_drop, df_risk, df_re, df_ra):
        # 获取京东渠道不去重的数据
        df_no_drop = df_no_drop[df_no_drop.归属渠道=='京东渠道']
        df_no_drop = df_no_drop.merge(df_risk[['trace_id', 'status_r']], on='trace_id', how='left').merge(
            df_re[['trace_id', 'status_re']], on='trace_id', how='left').merge(
            df_ra[['order_id', 'time_ra', 'status_ra']], left_on=['order_id', '下单日期'],
            right_on=['order_id', 'time_ra'], how='left')
        df_no_drop.loc[:, '风控通过件'] = np.where((df_no_drop.进件=='进件')&(df_no_drop.status_r!='1')&
                                                ~((df_no_drop.进件=='进件')&(~df_no_drop.电审拒绝原因.isna())&(df_no_drop.status2=='已退款')), 1, 0)
        df_no_drop.loc[:, '进件数'] = np.where(df_no_drop.进件=='进件', 1, 0)
        df_no_drop.loc[:, '待发货订单数'] = np.where(df_no_drop.status==2, 1, 0)
        df_no_drop.loc[:, '发货订单数'] = np.where(df_no_drop.status.isin([3, 4, 5, 6, 8, 15]), 1, 0)
        df_no_drop1 = df_no_drop.groupby('下单日期').agg({'order_id': 'count', '进件数': 'sum', '风控通过件': 'sum', '待发货订单数': 'sum', '发货订单数':'sum', 'new_actual_money': 'mean'}).rename(columns={'order_id': '订单数', 'new_actual_money': '买断价均值'})
        df_no_drop2 = df_no_drop.groupby('下单日期').agg({'new_actual_money': 'sum'}).rename(columns={'new_actual_money': '合计买断价'})
        df_no_drop_g = df_no_drop1.merge(df_no_drop2, on='下单日期', how='inner')
        df_no_drop_g.loc[:, '审核通过率'] = (df_no_drop_g.风控通过件/df_no_drop_g.订单数).map(lambda x: format(x, '.2%'))
        df_no_drop_g.loc[:, '风控通过率'] = (df_no_drop_g.风控通过件/df_no_drop_g.进件数).map(lambda x: format(x, '.2%'))
        df_no_drop_g.loc[:, '发货率'] = (df_no_drop_g.发货订单数/df_no_drop_g.订单数).map(lambda x: format(x, '.2%'))

        df_no_drop_g = df_no_drop_g[['订单数', '进件数', '风控通过件', '审核通过率', '风控通过率', '待发货订单数', '发货订单数', '发货率', '买断价均值', '合计买断价']]
        return df_no_drop_g

    # 调用运行函数
    def run(self, hour):
        print('正在查询数据...')
        df_order, df_risk_examine, df_ck, df_risk, df_re, df_ra = self.select_data(hour)
        print('数据查询完毕...\n正在清理数据...')
        df_contain, df, df2, dfck, df_j, df_ll, df2_ll, df_no_drop = self.clean_data(df_order,df_ck, df_risk, df_re, df_ra)
        print('数据清理完毕...\n正在获取数据...')
        df_all2, df_ss_group2, df_zm_group2 = self.get_data_hour(df, df2, df_risk_examine)
        df_lhyy_group, info = self.get_lhyy(df_ll, df2_ll, df_risk_examine)
        df_jd_group_new = self.jd(df_no_drop, df, df2, df_risk_examine)
        df_jd_group_new2 = self.jd2(df_no_drop, df, df2, df_risk_examine)
        df_jd_group_new3 = self.jd3(df_no_drop, df_risk, df_re, df_ra)
        print('数据获取完毕...')
        return df_all2, df_ss_group2, df_zm_group2,df, df2,df_risk_examine, df_lhyy_group, info, df_jd_group_new, df_jd_group_new2, df_jd_group_new3

    # # 设置钉钉机器人发送消息,联合运营（）
    # def send_dingtalk_message(self, webhook, secret, message):
    #     # 计算签名（如果有设置）
    #     if secret:
    #         timestamp = str(round(time.time() * 1000))
    #         string_to_sign = '{}\n{}'.format(timestamp, secret)
    #         hmac_code = hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'),
    #                              digestmod=hashlib.sha256).digest()
    #         sign = base64.b64encode(hmac_code).decode('utf-8')
    #         webhook = f'{webhook}&timestamp={timestamp}&sign={sign}'
    #
    #     # 构造消息体
    #     data = {
    #         "msgtype": "text",
    #         "text": {
    #             "content": message
    #         }
    #     }
    #
    #     # 发送请求
    #     try:
    #         response = requests.post(webhook, json=data)
    #         response.raise_for_status()
    #         print("消息发送成功")
    #     except requests.exceptions.RequestException as e:
    #         print(f"消息发送失败: {e}")


    # 创建定时任务
    def my_job(self, hour, minute, path, path2, hour_date):
        Today = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today}的{hour}:{minute}')
        df_all2, df_ss_group2, df_zm_group2,df, df2,df_risk_examine, df_lhyy_group, info, df_jd_group_new, df_jd_group_new2, df_jd_group_new3 = self.run(hour_date)
        with pd.ExcelWriter(path + f'渠道转化_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_all2.to_excel(writer, sheet_name='总体')
            df_ss_group2.to_excel(writer, sheet_name='搜索渠道')
            df_zm_group2.to_excel(writer, sheet_name='芝麻租物')
        with pd.ExcelWriter(path2 + f'联合运营_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_lhyy_group.to_excel(writer, sheet_name='联合运营转化')
        # 通过钉钉机器人发送消息
        # 要发送的消息内容
        # self.send_dingtalk_message(self.webhook, self.secret, info)
        del df_all2, df_ss_group2, df_zm_group2,df, df2,df_risk_examine, df_lhyy_group, info, df_jd_group_new, df_jd_group_new2, df_jd_group_new3
        gc.collect()
        print("回收内存执行完毕！\n")
    # def my_job2(self, path, hour_date):
    #     Today = str(datetime.now().strftime('%Y%m%d%H'))
    #     print('现在执行的是定时任务2...')
    #     df_all2, df_ss_group2, df_zm_group2,df, df2,df_risk_examine, df_lhyy_group, info = self.run(hour_date)
    #     self.get_zw(df, df2,df_risk_examine, path, Today)

    def my_job_jd(self, hour, minute, path, hour_date):
        Today = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today}的{hour}:{minute}')
        print('现在执行的是京东定时任务...')
        df_all2, df_ss_group2, df_zm_group2, df, df2, df_risk_examine, df_lhyy_group, info, df_jd_group_new, df_jd_group_new2, df_jd_group_new3 = self.run(hour_date)
        with pd.ExcelWriter(path + f'京享租_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_jd_group_new.to_excel(writer, sheet_name='京享租右卡', index=False)
            df_jd_group_new2.to_excel(writer, sheet_name='京东618活动', index=False)
            df_jd_group_new3.to_excel(writer, sheet_name='不去重京东数据')

        del df_all2, df_ss_group2, df_zm_group2, df, df2, df_risk_examine, df_lhyy_group, info, df_jd_group_new, df_jd_group_new2, df_jd_group_new3
        gc.collect()
        print("回收内存执行完毕！\n")


if __name__ == '__main__':
    hour = 18
    minute = 5
    # hour2 = 10
    # minute2 = 6
    hour_jd = 18
    minute_jd = 20
    path = r'\\digua\迪瓜租机\19.小程序发货率/'
    path1 = r'\\digua\迪瓜租机\19.全域租物/'
    path2 = r'\\digua\迪瓜租机\23.芝麻联合运营/'
    path_jd = r'\\digua\迪瓜租机\24.京东数据/'
    ch = Channel()
    # ch.run(24)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天18点9分开始执行
    job = scheduler.add_job(ch.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path, path2,  18])
    # job2 = scheduler.add_job(ch.my_job2, 'cron', hour=hour2, minute=minute2, args=[path1, 18])
    #每天18点20分开始执行
    job_jd = scheduler.add_job(ch.my_job_jd, 'cron', hour=hour_jd, minute=minute_jd, args=[hour_jd, minute_jd, path_jd, 24])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # ch.my_job(hour, minute, path, path2,  18)
    # ch.my_job_jd(hour_jd, minute_jd, path_jd, 24)
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
            # next_run_time1 = job2.next_run_time
            next_run_time_jd = job_jd.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            elif next_run_time_jd:
                now = datetime.now(timezone.utc)
                sleep_duration_jd = (next_run_time_jd - now).total_seconds()
                if sleep_duration_jd > 0:
                    time.sleep(sleep_duration_jd)
            # elif next_run_time1:
            #     now = datetime.now(timezone.utc)
            #     sleep_duration1 = (next_run_time1 - now).total_seconds()
            #     if sleep_duration1 > 0:
            #         time.sleep(sleep_duration1)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
        gc.collect()