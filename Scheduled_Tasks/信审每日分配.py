from time import sleep

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

class Task_Allocation:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        # 实例化All_Model类
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()


    # 查询近一天24点前的数据（日报格式）
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
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.is_vip') end,'"','') as is_vip
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.status') end,'"','') as status_result
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.queue_verify_type') end,'"','') as qvt_risk
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.queue_verify_type') end,'"','') as qvt_result 
        ,cc.name as channel_name         -- 来源渠道
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_id,om.merchant_name
        ,topay.total_freeze_fund_amount , tod.dy_order_item_json, pa.type
        ,om.buy_service_product,tso.status as service_status 
        ,om.order_method, om.activity_id, om.appid, tprm.max_overdue_days
        , tor.update_time, tomt.reason, cc.channel_type_id, tomt.merchant_id merchant_id_t, om.order_type
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
        and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -1 day )
        -- and  om.create_time <= DATE_ADD(CURRENT_DATE,INTERVAL -10 day )
        and hour(om.create_time)<'{hour}'
        -- and date_format(om.create_time, '%Y-%m-%d')>='2025-03-01'
        ;
        '''
        df_order = self.clean.query(sql1)
        df_order = df_order[df_order.type != 4]

        sql3 = ''' -- 拒量拒绝原因
        SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status   FROM `db_credit`.risk_examine
        '''
        df_risk_examine = self.clean.query(sql3)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)

        sql_risk = ''' -- risk等级
        select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r from db_credit.risk
        '''
        df_risk = self.clean.query(sql_risk)

        sql_ra = ''' -- 996强拒表
        select order_id, time, replace(case when JSON_VALID(result) THEN JSON_EXTRACT(result, '$.status') end,'"','') as status_ra  from db_credit.risk_alipay_interactive_prod_result
        '''
        df_ra = self.clean.query(sql_ra)
        df_ra.loc[:, 'time_ra'] = pd.to_datetime(df_ra.time.dt.date, errors="coerce")
        df_ra = df_ra.sort_values(by='time', ascending=False).groupby('order_id').head(1)

        sql_name = '''
        SELECT tuvor.order_id, tu.nick_name 分配人, tuvor.update_time 
        FROM db_digua_business.t_user_verify_order_record tuvor
        left join db_digua_business.t_user tu on tuvor.user_id = tu.id 
        where tuvor.del_flag = 0 ORDER BY tuvor.update_time
        '''
        df_name = self.clean.query(sql_name)

        f_path_ck = "F:/myfile/p站数据/台账数据/维客壹佰2025年台账.xlsx"
        df_ck = pd.read_excel(f_path_ck, sheet_name="2025")
        return df_order, df_risk_examine, df_ck, df_risk, df_re, df_ra, df_name

    # 数据处理
    def clean_data(self, df, df_ck, df_risk, df_re, df_ra, df_name):
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
        # try:
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()
        # except:
        #     df["无法联系原因"] = ''
        df.loc[:, "商品类型"] = np.where(
            df["product_name"].str.contains('99新') | df["product_name"].str.contains('95新') | df[
                "product_name"].str.contains('准新'), "二手", "全新")  ##  准新
        df.loc[:, "租赁方案"] = np.where(df["sku_attributes"].str.contains('租完即送'), "租完即送", "租完归还")
        df.loc[:, "押金类型"] = np.where(df["total_freeze_fund_amount"] > 0, "部分免押", "全免押")
        df.loc[:, "优惠券使用否"] = np.where(
            (df["new_actual_money"] - df["all_money"] > 0) & (df["租赁方案"] == '租完即送'), "已使用", "未使用")
        df.loc[:, "是否进行预授权"] = np.where(df["total_freeze_fund_amount"].isnull(), "未预授权", "已预授权")
        df.loc[:, "颜色"] = df.apply(lambda x: self.clean.getcolor(x["sku_attributes"]), axis=1)
        df.loc[:, "内存"] = df.apply(lambda x: self.clean.getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'], x['order_type']), axis=1)
        # 订单去重
        df = self.clean.order_drop_duplicates(df)
        # 定义状态
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
        df = self.clean.status_node(df)
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
        df2 = self.clean.drop_rejected_merchant(df2)

        return df_contain, df, df2, dfck, df_j

    def rate(selc, df2_all_merge):
        # 标记通过件、碎屏险
        df2_all_merge.loc[:, '通过量'] = np.where(df2_all_merge.status2.isin(['待发货', '待收货', '租赁中', '已完成']), 1, 0)
        df2_all_merge.loc[:, '碎屏险数量'] = np.where(df2_all_merge.service_status.isin([2, 3]), 1, 0)
        # 按name分组聚合
        df2_all_merge_group = df2_all_merge.groupby('name').agg(
            {'order_id': 'count', '通过量': 'sum', '出库前风控强拒': 'sum', '人审拒绝': 'sum', '待审核': 'sum',
                '客户取消': 'sum', '已退款': 'sum', '碎屏险数量': 'sum'}).rename(columns={'order_id': '处理量', '出库前风控强拒': '强拒量'})
        # print(df2_all_merge_group)
        df2_all_merge_group.loc[:, '取消量'] = df2_all_merge_group.客户取消 + df2_all_merge_group.已退款
        df2_all_merge_group.loc[:, '通过率'] = (df2_all_merge_group.通过量 / df2_all_merge_group.处理量).map(
            lambda x: format(x, '.2%'))
        df2_all_merge_group.loc[:, '取消率'] = (df2_all_merge_group.取消量 / df2_all_merge_group.处理量).map(
            lambda x: format(x, '.2%'))
        df2_all_merge_group = df2_all_merge_group[
            ['处理量', '通过量', '强拒量', '通过率', '取消量', '取消率', '碎屏险数量']]
        return df2_all_merge_group

    def get_data(self, df, df2, df_risk_examine, df_name, month=None):
        Today = str(datetime.now().strftime('%Y-%m-%d'))
        df = df[df.下单日期== Today]
        df2 = df2[df2.下单日期 == Today]
        # df = df[df.下单日期==(pd.to_datetime(Today) - timedelta(days=1))]
        # df2 = df2[df2.下单日期==(pd.to_datetime(Today) - timedelta(days=1))]
        # df = df[df.下单月份==month]
        # df2 = df2[df2.下单月份==month]
        # 匹配小蚂蚁（商家拒量）数据
        df_xmy = df[df['merchant_name'].isin(['小蚂蚁租机', '兴鑫兴通讯', '崇胜数码','喜卓灵租机'])]
        # 出库前强拒数据重命名
        df_risk_examine.rename(columns={'time': 'time_risk_ex', 'status': 'status_risk_ex'}, inplace=True)
        # 对小蚂蚁数据和出库前强拒数据进行拼接
        df_risk_examine_all = pd.merge(df_xmy, df_risk_examine, on='trace_id', how='inner')
        # 计算出库前强拒的订单数
        df_risk_examine_all2 = df_risk_examine_all[
            (df_risk_examine_all['time_risk_ex'] < df_risk_examine_all['update_time'])]
        # 进行排序并取到最近的一个订单
        df_risk_examine_all2 = df_risk_examine_all2.sort_values(['order_id', 'time_risk_ex'],
                                                                ascending=[True, False]).groupby('order_id').head(1)
        df_risk_examine_all2 = df_risk_examine_all2[df_risk_examine_all2['status_risk_ex'] == '1']
        # 排除出库前风控强拒的订单
        df_xmy_new = df_xmy[~df_xmy['order_id'].isin(df_risk_examine_all2['order_id'].to_list())]
        # 定义机审强拒和人审拒绝
        df_xmy_new['小蚂蚁机审强拒'] = np.where(df_xmy_new['reason'] == '系统风控拒绝转移', 1, 0)
        df_xmy_new['小蚂蚁人审拒绝'] = np.where(df_xmy_new['reason'] != '系统风控拒绝转移', 1, 0)
        # 出库前风控强拒
        df_risk_examine_all2 = df_risk_examine_all2[['order_id', '出库前风控强拒', '人审拒绝', '待审核', 'status2', '客户取消', '已退款', 'service_status']]
        # 小蚂蚁人审拒绝
        df_xmy_new = df_xmy_new[df_xmy_new.小蚂蚁人审拒绝==1][['order_id', '出库前风控强拒', '人审拒绝', '待审核', 'status2', '客户取消', '已退款', 'service_status']]
        # 机审通过件
        df2_new = df2[df2.机审通过件==1]
        df2_new = df2_new[['order_id', '出库前风控强拒', '人审拒绝', '待审核', 'status2', '客户取消', '已退款', 'service_status', 'is_vip', 'status_result']]
        # concat拼接
        if len(df_risk_examine_all2) > 0:
            df2_all = pd.concat([df2_new, df_xmy_new, df_risk_examine_all2])
        else:
            df2_all = pd.concat([df2_new, df_xmy_new])
        # 获取订单的最后一位分配人并关联分配数据
        df_name_new = df_name.sort_values('update_time', ascending=False).groupby('order_id').head(1)
        df2_all_merge = df2_all.merge(df_name_new[['order_id', '分配人']], on='order_id', how='left')
        # 创建分配人映射关系
        name_dict = {
            '小张': '李巧玲',
            '小周': '李巧凤',
            '小南': '刘三妹',
            '小何': '何静',
            '小谢': '谢金凤',
            '小咪': '杨健',
            '小慧': '林思慧',
            '小滢': '胡彩滢',
            '小星': '廖丽敏',
            '小兰': '黄兰娟',
            '小晚': '周莹',
            '小芳': '罗芳'
        }
        name_list = ['罗文龙', '何静', '刘三妹', '杨健', '林思慧', '胡彩滢', '周汉鸿', '廖丽敏', '黄兰娟', '周莹', '邹巧巧', '冯二洋','罗芳', '魏朵','周念慈',]
        # 匹配映射字典
        # df2_all_merge = df2_all_merge[~df2_all_merge['分配人'].isna()]
        df2_all_merge.loc[:, 'name'] = df2_all_merge.分配人.apply(lambda x: name_dict[x] if str(x).startswith('小') and str(x) in name_dict.keys() else x)
        df2_all_merge = df2_all_merge[df2_all_merge.name.isin(name_list)]
        # 计算数据
        df2_all_merge_group = self.rate(df2_all_merge)
        # 计算免审数据
        # df_ms = df2_all_merge[(df2_all_merge.is_vip == '1') & (df2_all_merge.status_result == '0')]
        # df_ms_group = self.rate(df_ms)
        # df_ms_group = df_ms_group.rename(columns={'处理量': '免审进件', '通过量': '免审出库', '通过率': '免审进件出库率'})
        # df2_all_merge_group = df2_all_merge_group[['处理量', '通过量', '通过率']].merge(df_ms_group[['免审进件', '免审出库', '免审进件出库率']], on='name', how='left')
        # df2_all_merge_group.loc[:, '非免审进件'] = df2_all_merge_group.处理量-df2_all_merge_group.免审进件
        # df2_all_merge_group.loc[:, '非免审出库'] = df2_all_merge_group.通过量-df2_all_merge_group.免审出库
        # df2_all_merge_group.loc[:, '非免审进件出库率'] = (df2_all_merge_group.非免审出库/df2_all_merge_group.非免审进件).map(lambda x: format(x, '.2%'))
        # df2_all_merge_group = df2_all_merge_group[['免审进件','免审出库','免审进件出库率', '非免审进件', '非免审出库', '非免审进件出库率','处理量', '通过量', '通过率']].rename(columns={'处理量': '免审进件+非免审进件', '通过量': '免审出库+非免审出库', '通过率': '总体转化'})
        # df2_all_merge_group.loc['nan', :] = np.nan
        return df2_all_merge_group#, df_ms_group


    def run(self, hour):
        print('正在查询数据...')
        df_order, df_risk_examine, df_ck, df_risk, df_re, df_ra, df_name = self.select_data(hour)
        print('数据查询完毕...\n正在清理数据...')
        df_contain, df, df2, dfck, df_j = self.clean_data(df_order, df_ck, df_risk, df_re, df_ra, df_name)
        print('数据清理完毕...\n正在获取数据...')
        df2_all_merge_group = self.get_data(df, df2, df_risk_examine, df_name)
        # df2_all_merge_group = self.get_data(df, df2, df_risk_examine, df_name, '2025-03')
        # df2_all_merge_group2 = self.get_data(df, df2, df_risk_examine, df_name, '2025-04')
        # df2_all_merge_group3 = self.get_data(df, df2, df_risk_examine, df_name, '2025-05')
        # df2_all_merge_group4 = self.get_data(df, df2, df_risk_examine, df_name, '2025-06')
        # df2_all_merge_group = pd.concat([df2_all_merge_group, df2_all_merge_group2, df2_all_merge_group3, df2_all_merge_group4], axis=0)
        print('数据获取完毕...')
        return df2_all_merge_group#, df2_all_merge_group2, df2_all_merge_group3, df2_all_merge_group4, df_ms_group, df_ms_group2, df_ms_group3, df_ms_group4

    # 创建定时任务
    def my_job(self, hour, minute, path, hour_date):
        Today = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today}的{hour}:{minute}')
        df2_all_merge_group = self.run(hour_date)
        with pd.ExcelWriter(path + f'信审每日分配_{Today}.xlsx', engine='xlsxwriter') as writer:
            df2_all_merge_group.to_excel(writer, sheet_name='总体')
        # df2_all_merge_group, df2_all_merge_group2, df2_all_merge_group3, df2_all_merge_group4, df_ms_group, df_ms_group2, df_ms_group3, df_ms_group4 = self.run(hour_date)
        # with pd.ExcelWriter(path + f'信审每日分配_{Today}.xlsx', engine='xlsxwriter') as writer:
        #     df2_all_merge_group.to_excel(writer, sheet_name='总体_3月')
        #     df2_all_merge_group2.to_excel(writer, sheet_name='总体_4月')
        #     df2_all_merge_group3.to_excel(writer, sheet_name='总体_5月')
        #     df2_all_merge_group4.to_excel(writer, sheet_name='总体_6月')
        #     df_ms_group.to_excel(writer, sheet_name='免审_3月')
        #     df_ms_group2.to_excel(writer, sheet_name='免审_4月')
        #     df_ms_group3.to_excel(writer, sheet_name='免审_5月')
        #     df_ms_group4.to_excel(writer, sheet_name='免审_6月')



if __name__ == '__main__':
    hour = 21
    minute = 4
    path = r'\\digua\迪瓜租机\13.运营-信审每日数据/'
    ta = Task_Allocation()
    # ta.my_job(hour, minute, path, 24)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天的21点4分执行一次
    job = scheduler.add_job(ta.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path,  24])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # 实时执行
    # ta.my_job(hour, minute, path, 24)
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