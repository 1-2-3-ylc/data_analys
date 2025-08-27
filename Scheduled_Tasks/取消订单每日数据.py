import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler

# import seaborn as sns
#sns.set(style="darkgrid")
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False


from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import time
import gc
import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
import time
from datetime import timedelta ,datetime, timezone
import re
import os
import sys

from Class_Model.All_Class import All_Model, Week_Model, Data_Clean


class Cancel_Order:
    def __init__(self):
        # 初始化类
        pd.set_option('display.max_columns', None)
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()

    # 查询近15天的数据（日报）
    def get_data(self):
        sql1 = ''' -- 订单&风控信息  近10日数据   
        SELECT date(om.create_time) as create_date,om.create_time,om.id as order_id ,om.order_number,om.all_money 
        ,om.status
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
        ,cc.channel_type_id
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_id,om.merchant_name
        ,topay.total_freeze_fund_amount 
        ,om.buy_service_product,tso.status as service_status 
        ,om.order_method, pa.type, om.order_type
        from  db_digua_business.t_order  om 
        left join db_digua_business.t_postlease_receivables_monitoring   tprm  on tprm.order_id=om.id
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
        where om.user_mobile is not null 
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静'
        ,'陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静')
        and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -15 day )
        ;
        '''
        sql_risk = ''' -- risk等级
                select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r 
                , replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.union_rent_tag') end,'"','') as union_rent_tag
                , replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.union_rent_rejected') end,'"','') as union_rent_rejected
                from db_credit.risk
                '''
        sql3 = '''
                SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status   FROM `db_credit`.risk_examine
                '''
        df_risk_examine = self.clean.query(sql3)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)
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

        df_order = self.clean.query(sql1)
        return df_order, df_risk, df_re, df_ra, df_name

    # 处理数据
    def clean_data(self, df, df_risk, df_re, df_ra, df_name):
        # 处理日期
        df.loc[:, "下单日期"] = pd.to_datetime(df["create_time"]).dt.date
        df.loc[:, "下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df.loc[:, "月份"] = pd.to_datetime(df["下单日期"]).dt.month
        # 处理备注信息
        df.loc[:, '拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        df = df[df['sku_attributes'].notnull()]

        df.loc[:, "取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df.loc[:, "电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df.loc[:, "无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].astype(str).str.split("$").str[
            0].str.strip()
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
        df.loc[:, '免审'] = np.where(df.decision_result.str.contains(pat='免人审', regex=False), 1, 0)
        df.loc[:, "归属渠道"] = df.apply(lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'], x['order_type']),axis=1)
        # 订单去重
        df = self.clean.order_drop_duplicates(df)
        # 状态定义
        df_ra['time_ra'] = pd.to_datetime(df_ra.time_ra)
        df['下单日期'] = pd.to_datetime(df.下单日期)
        df = df.merge(df_risk[['trace_id', 'status_r', 'union_rent_tag', 'union_rent_rejected']], on='trace_id', how='left').merge(
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
        df.loc[:, "审核状态"] = df.apply(lambda x: self.clean.reject_type(x["拒绝理由"], x["进件"], x["电审拒绝原因"], x["取消原因"], x["status2"],x["无法联系原因"], x["total_describes"], x['是否前置拦截'], x['是否机审强拒'],x['是否出库前风控强拒']), axis=1)
        # 删除商家数据
        df = self.clean.drop_merchant(df)
        # 各个节点状态
        df = self.clean.status_node(df)
        # 剔除据量数据
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        # 获取订单的最后一位分配人并关联分配数据
        df_name_new = df_name.sort_values('update_time', ascending=False).groupby('order_id').head(1)
        df2_all_merge = df2.merge(df_name_new[['order_id', '分配人']], on='order_id', how='left')
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
        name_list = ['李巧玲', '李巧凤', '刘三妹', '何静', '谢金凤', '廖丽敏', '李楠', '黄兰娟', '杨健', '林思慧','胡彩滢', '周莹', '罗芳', '周念慈', '周汉鸿']
        # 匹配映射字典
        df2_all_merge.loc[:, 'name'] = df2_all_merge.分配人.apply(
            lambda x: name_dict[x] if str(x).startswith('小') and str(x) in name_dict.keys() else x)
        return df2_all_merge

    def cancel(self, df, days1, days2):
        # 进件订单
        jj_order = df.query('是否进件==1')
        # 机审强拒订单
        jsqj_order = df.query('机审强拒==1')
        # 获取机jj_order中不在jsqj_order的订单
        jsqj_unique = jj_order.index.difference(jsqj_order.index)
        # 使用这些字段重新索引jj_order
        jsqj_orders = jj_order.reindex(jsqj_unique)

        # 出库订单
        ck_order = df.query('是否出库==1')
        # 获取机df_jsqj_order中不在ck_order的订单
        jsqj_unique = jsqj_orders.index.difference(ck_order.index)
        # 使用这些字段重新索引df_jsqj_order
        jsqj_orders = jsqj_orders.reindex(jsqj_unique)

        # # 出库前风控强拒订单
        df_fkqj_order = df.query('出库前风控强拒==1')
        # # 获取机jsqj_order中不在df_fkqj_order的订单
        jsqj_unique = jsqj_orders.index.difference(df_fkqj_order.index)
        # # 使用这些字段重新索引df_jsqj_order
        jsqj_orders = jsqj_orders.reindex(jsqj_unique)

        # # 人审拒绝订单
        rs_order = df.query('人审拒绝==1')
        # # 获取机jsqj_order中不在rs_order的订单
        jsqj_unique = jsqj_orders.index.difference(rs_order.index)
        # # 使用这些字段重新索引df_jsqj_order
        jsqj_orders = jsqj_orders.reindex(jsqj_unique)

        order_qx_all = jsqj_orders.query('status2=="已退款"')[['create_time', 'order_number', 'user_mobile', 'cancel_reason', 'rejected', 'total_describes', 'name']]
        order_qx_all.loc[:, '取消原因'] = order_qx_all.cancel_reason.str.split('：').str[-1].str.split('"').str[0]
        order_qx_all.rename(columns={'create_time': '订单日期', 'order_number': '订单号', 'user_mobile': '手机号','rejected': '拒绝原因'}, inplace=True)

        order_qx_all['日期'] = pd.to_datetime(order_qx_all['订单日期'].dt.strftime('%Y-%m-%d'))
        seven_ago = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - timedelta(days=7)
        one_ago = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - timedelta(days=days1)
        today = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - timedelta(days=days2)
        order_qx_all = order_qx_all[(order_qx_all['日期'] >= one_ago) & (order_qx_all['日期'] <= today)]

        # 人审拒绝
        df_rsjj = df[(df['人审拒绝'] == 1) & (df['status2'] != '待审核') & (df['type'] != 4)
                    & (df['status2'] != '待发货')][['create_time', 'order_number', 'user_mobile', '电审拒绝原因']].rename(columns={'create_time': '订单日期', 'order_number': '订单号', 'user_mobile': '手机号'})

        # 联合运营
        df_lhyy = df[df.union_rent_tag=='1']
        df_lhyy.loc[:, '联合运营取消原因'] = df_lhyy.union_rent_rejected.str.strip('[]')

        # 曙光/旭日计划
        order_qx_all_play = order_qx_all[order_qx_all.取消原因.str.contains('曙光/旭日计划')==True]


        return order_qx_all, df_rsjj, df_lhyy, order_qx_all_play


    def run(self):
        print('正在查询数据...')
        df, df_risk, df_re, df_ra, df_name = self.get_data()
        print('数据查询完毕...\n正在进行数据清理...')
        df_new = self.clean_data(df, df_risk, df_re, df_ra, df_name)
        print('数据清理完毕...\n正在获取数据分组...')
        order_qx_all, df_rsjj, df_lhyy, order_qx_all_play = self.cancel(df_new, 5, 0)
        print('数据获取完毕...\n请查看数据...')
        return order_qx_all, df_rsjj, df_lhyy, order_qx_all_play

    def my_job(self, hour, minute, path):
        Today = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today}的{hour}:{minute}')
        order_qx_all, df_rsjj, df_lhyy, order_qx_all_play = self.run()
        with pd.ExcelWriter(path + f'取消订单数据_{Today}.xlsx', engine='openpyxl') as writer:
            order_qx_all.to_excel(writer, sheet_name='取消订单', index=False)
            df_rsjj.to_excel(writer, sheet_name='电审拒绝', index=False)
            df_lhyy.to_excel(writer, sheet_name='联合运营', index=False)
            order_qx_all_play.to_excel(writer, sheet_name='曙光旭日计划', index=False)

        del order_qx_all, df_rsjj, df_lhyy, order_qx_all_play
        gc.collect()
        print("回收内存执行完毕！\n")


if __name__ == '__main__':
    hour = 10
    minute = 45
    path = r'\\digua\迪瓜租机\13.运营-信审每日数据/'
    co = Cancel_Order()
    # 实时
    # co.my_job(hour, minute, path)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天的10点45开始执行
    job = scheduler.add_job(co.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # 实时
    # co.my_job(hour, minute, path)
    # print('定时任务执行完毕,请查看数据...')
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
        gc.collect()





