from operator import index

import pandas as pd
import numpy as np
import pymysql
import warnings
import gc
import json


from apscheduler.triggers.cron import CronTrigger
from dateutil.utils import today

warnings.filterwarnings('ignore')

from datetime import datetime, timedelta, timezone
import time
from apscheduler.schedulers.background import BackgroundScheduler
from Class_Model.All_Class import All_Model, Data_Clean

class Phone_Name:
    def __init__(self):
        self.all_model = All_Model()
        self.clean = Data_Clean()



    # 查询数据
    def select_data(self):
        sql1 = f''' -- 订单&风控信息  近10日数据   
                SELECT om.create_time,om.id as order_id ,om.order_number,om.status, date(om.create_time) as create_date
                ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
                when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
                when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" else "其他订单" end as status2 
                ,tod.sku_attributes,tod.product_name,tod.new_actual_money
                ,om.user_mobile,tmu.true_name,tmu.id_card_num
                ,top.total_describes
                ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.traceid') end,'"','') as trace_id 
                ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.rejected') end,'"','') as rejected 
                ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.result') end,'"','') as result 
                ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.tips') end,'"','') as tips  
                ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.is_vip') end,'"','') as is_vip
                ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.status') end,'"','') as status_result
                ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.queue_verify_type') end,'"','') as qvt_risk
                ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.queue_verify_type') end,'"','') as qvt_result
                ,cc.name as channel_name         -- 来源渠道
                ,cc.channel_type_id              -- 渠道id
                ,pa.name as activity_name        -- 活动名称
                ,om.merchant_id,om.merchant_name,pa.type
                ,om.order_method, om.activity_id
                ,om.order_type, tor.update_time, tomt.reason, tpmn.name 机型, cc.scene, tp.id 商品ID, tp.name phone_name
                -- ,tolog.status 物流状态
                from  db_digua_business.t_order  om
                left join db_digua_business.t_postlease_receivables_monitoring tprm on tprm.order_id=om.id
                left join db_digua_business.t_order_risk tor on om.id = tor.order_id
                -- 备注信息合并 
                left join ( SELECT  t.order_id,JSON_ARRAYAGG(t.describes) as total_describes from db_digua_business.t_order_personnel t   GROUP BY 1 ) top 
                on om.id = top.order_id 
                -- 渠道名称
                left join db_digua_business.t_channel cc on om.channel = cc.scene 
                -- 活动名称
                left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
                -- 用户信息 
                left join db_digua_business.t_member_user tmu on om.user_id = tmu.id
                -- 商品信息
                left join db_digua_business.t_order_details tod on om.id = tod.order_id
                -- 商家订单转移表
                left join db_digua_business.t_order_merchant_transfer tomt on tomt.order_id=om.id
                -- 商品表
                left join db_digua_business.t_product tp on tp.id = tod.product_id
                -- 商品型号
                left join db_digua_business.t_product_model_number tpmn on tpmn.id=tp.model_number_id
                -- 物流表
                left join db_digua_business.t_order_logistics tolog on tolog.order_id=om.id
                where om.user_mobile is not null 
                
                -- 获取近15天数据，加上当天
                and om.create_time >= date_sub(CURRENT_DATE, INTERVAL 14 DAY)
                -- and hour(om.create_time)<
                -- and date_format(om.create_time, '%Y-%m-%d')>='2025-10-08'
                -- and date_format(om.create_time, '%Y-%m-%d')<='2025-10-14'
                ;
                '''
        df_order = self.clean.query(sql1)
        df_order = df_order[df_order.type != 4]
        sql_risk = ''' -- risk等级
                        select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r 
                        from db_credit.risk
                        '''
        df_risk = self.clean.query(sql_risk)
        sql3 = '''
                    SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status   FROM `db_credit`.risk_examine
                    '''
        df_risk_examine = self.clean.query(sql3)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)

        sql_ra = ''' -- 996强拒表
                    select order_id, time, replace(case when JSON_VALID(result) THEN JSON_EXTRACT(result, '$.status') end,'"','') as status_ra  from db_credit.risk_alipay_interactive_prod_result
                    '''
        df_ra = self.clean.query(sql_ra)
        df_ra.loc[:, 'time_ra'] = pd.to_datetime(df_ra.time.dt.date, errors="coerce")
        df_ra = df_ra.sort_values(by='time', ascending=False).groupby('order_id').head(1)

        sql_upv = '''
        select
        tsc.scene, date(tsc.day) c_date, tsc.new_uv_count, tsc.uv_count, tsc.pv_count
        from db_digua_business.t_statistics_channel tsc
        '''
        df_upv = self.clean.query(sql_upv)
        df_order.loc[:, 'phone_name'] = df_order.phone_name.str.replace(' ', '').str.extract(r'(iPhone\d+(ProMax|Pro|Plus)?)')[0]
        df_order = df_order.merge(df_upv, left_on=['scene', 'create_date'], right_on=['scene', 'c_date'], how='left')
        # df_order.loc[:,'商品ID'] = df_order.商品ID.astype(str)+'_'+df_order.phone_name



        return df_order, df_risk, df_risk_examine, df_re, df_ra

    # 数据清理
    def clean_data(self, df, df_risk, df_re, df_ra, key=None):
        # 日期处理
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")

        df["月份"] = df["下单日期"].dt.month
        df["下单月份"] = df["create_time"].dt.strftime('%Y-%m')

        df['hour'] = df['create_time'].dt.hour
        # 备注信息处理
        df['拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        df = df[df['sku_attributes'].notnull()]
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()

        df.loc[:, "内存"] = df.apply(lambda x: self.clean.getneicun(x["sku_attributes"]), axis=1)
        # df.loc[:, '机型内存'] = df.机型+'_'+df.内存
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"],
                                            x['channel_type_id'], x['order_type']), axis=1)
        # 京东不去重数据，不需要执行订单去重处理
        # if key != '京东':
        #     # 订单去重处理
        df = self.clean.order_drop_duplicates(df)

        # 定义状态处理
        df = df.merge(df_risk[['trace_id', 'status_r']], on='trace_id', how='left').merge(
            df_re[['trace_id', 'status_re']], on='trace_id', how='left').merge(
            df_ra[['order_id', 'time_ra', 'status_ra']], left_on=['order_id', '下单日期'],
            right_on=['order_id', 'time_ra'], how='left')
        df['是否前置拦截'] = np.where(
            (df.result.str.contains('id_card不得为空')) | (df.result.str.contains('mobile校验不通过')) | (
                df.result.str.contains('name校验不通过'))
            | (df.result.str.contains('年龄超过49岁或低于18岁')) | (df.result == '风控未通过') | (
                    df.result == '黑名单用户'), 1, 0)
        df['是否机审强拒'] = np.where(df.status_r == '1', 1, 0)
        df['是否出库前风控强拒'] = np.where((df.status_re == '1') | (df.status_ra == '1'), 1, 0)
        df.loc[:, "审核状态"] = df.apply(
            lambda x: self.clean.reject_type(x["拒绝理由"], x["进件"], x["电审拒绝原因"], x["取消原因"], x["status2"],
                                            x["无法联系原因"], x["total_describes"], x['是否前置拦截'], x['是否机审强拒'],x['是否出库前风控强拒']), axis=1)
        # 剔除商家数据
        df = self.clean.drop_merchant(df)
        # 获取节点状态数据
        df = self.clean.status_node(df)

        # 剔除拒收数据
        # df = df[df['物流状态'] != 5]
        # 剔除据量数据
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        df2 = df2[df2.是否拒量 == 0]



        return df, df2






    def run(self):
        df, df_risk, df_risk_examine, df_re, df_ra = self.select_data()

        return df, df_risk, df_risk_examine, df_re, df_ra

    # 获取近15天出库top15的机型
    def get_top15(self, df, df2, df_risk_examine):
        #today = datetime.now() - timedelta(days=14)
        df = df#[df.下单日期 >= today]
        df2 = df2#[df2.下单日期 >= today]
        df_top15 = self.all_model.data_group(df, df2, df_risk_examine, '机型')[['出库']]
        df_top15 = df_top15.sort_values(by='出库', ascending=False)[:15].index.to_list()
        return df_top15

    def get_result(self, df, model):
        # 获取当天数据
        to_day = pd.to_datetime(datetime.now().date())
        df_today = df[df.下单日期 == datetime.now().strftime('%Y-%m-%d')]
        # print("df_today的下单日期", df_today.下单日期.unique())

        # 获取昨天的数据17点40前的数据
        yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
        yesterday_start = yesterday.normalize()  # 昨天开始时间 00:00:00
        yesterday_end = yesterday_start + pd.Timedelta(hours=17, minutes=40)  # 昨天17:40

        # 筛选昨天00:00到17:40之间的数据
        df_yesterday = df[
            (df.下单日期 >= yesterday_start) &
            (df.下单日期 <= yesterday_end)
            ]
        # print("df_yesterday的下单日期", df_yesterday.下单日期.unique())
        # 获取近7天含当天的数据
        df_7 = df[df.下单日期>(datetime.now()-timedelta(days=7))]
        # print("df_7的下单日期", df_7.下单日期.unique())
        # 根据model分组聚合
        df_today = df_today.groupby(model).agg({'order_id': 'size', '是否进件': 'sum'}).fillna(0)
        df_today.rename(columns={'order_id': '去重订单数', '是否进件': '进件数'}, inplace=True)
        df_yesterday = df_yesterday.groupby(model).agg({'order_id': 'size', '是否进件': 'sum'}).fillna(0)
        df_yesterday.rename(columns={'order_id': '去重订单数_昨日', '是否进件': '进件数_昨日'}, inplace=True)
        df_7 = df_7.groupby(model).agg({'order_id': 'size', '是否进件': 'sum'}).fillna(0)
        df_7.rename(columns={'order_id': '去重订单数_7天', '是否进件': '进件数_7天'}, inplace=True)
        # 基于model合并数据
        # df_result = df_today.merge(df_yesterday, on=model, how='left').merge(df_7, on=model, how='left')
        df_result = df_7.merge(df_yesterday, on=model, how='outer').merge(df_today, on=model, how='outer').fillna(0)# outer 全外连接
        # 按去重订单数降序排序
        df_result = df_result.sort_values(by='去重订单数_7天', ascending=False)
        df_result.loc[:, '订单增涨'] = df_result['去重订单数'] - df_result['去重订单数_昨日']
        df_result.loc[:, '进件增涨'] = df_result['进件数'] - df_result['进件数_昨日']

        return df_result

    # 统计近1
    def month_model(self, path):


        Today = str(datetime.now().strftime('%Y%m%d%H%M'))
        print(f'执行定时任务：现在是{Today}...')
        print('正在查询数据...')
        # 执行数据查询，获取多个DataFrame
        df, df_risk, df_risk_examine, df_re, df_ra = self.run()
        print('数据查询完毕！\n正在清理数据...')
        # 清理数据，返回两个DataFrame
        df, df2 = self.clean_data(df, df_risk, df_re, df_ra)
        print('数据清理完毕！\n正在计算机型转化数据...')
        df_top_15 = self.get_top15(df, df2, df_risk_examine)
        df = df[df.机型.isin(df_top_15)]
        print(df.机型.unique())


        # 筛选非二手数据
        # df = df[df.是否二手 == 0]
        # df2 = df2[df2.是否二手 == 0]

        # 总体数据处理
        df_model_group = self.get_result(df, '机型' )
        # 芝麻租物渠道数据处理
        print(df.下单日期.unique())
        df_zm = df[df.归属渠道=='芝麻租物']

        df_model_group_zm = self.get_result(df_zm, '机型')
        # 京东渠道数据处理
        df_jd = df[df.归属渠道 == '京东渠道']

        df_model_group_jd = self.get_result(df_jd, '机型')
        # 搜索渠道数据处理
        df_ss = df[df.归属渠道 == '搜索渠道']

        df_model_group_ss = self.get_result(df_ss, '机型')
        print('机型转化数据计算完毕！\n正在写入数据...')
        with pd.ExcelWriter(path + f'/近15天出库机型订单进件数_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_model_group.to_excel(writer, sheet_name='总体')
            df_model_group_zm.to_excel(writer, sheet_name='芝麻租物')
            df_model_group_jd.to_excel(writer, sheet_name='京东渠道')
            df_model_group_ss.to_excel(writer, sheet_name='搜索渠道')
        print('数据写入完毕！')
        del df_model_group, df_model_group_zm, df_model_group_jd, df_model_group_ss
        gc.collect()
        print("回收内存执行完毕！\n")


if __name__ == '__main__':
    PN = Phone_Name()
    hour = 18
    minute = 10
    path = r'\\digua\迪瓜租机\002数据监测\10.出库前十机型订单进件数'
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()

    # 新增任务：每天上午17点40分执行获取当月的机型转化数据
    job_day_current = scheduler.add_job(
        PN.month_model,'cron',
        hour=17, minute=40,
        args=[path]
    )

    PN.month_model(path)


    print('定时任务创建完毕...\n正在执行定时任务...')
    # 查看是否添加了任务
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:

            next_run_time_jd_day = job_day_current.next_run_time

            if next_run_time_jd_day:
                now = datetime.now(timezone.utc)
                sleep_duration_jd_month = (next_run_time_jd_day - now).total_seconds()
                if sleep_duration_jd_month > 0:
                    time.sleep(sleep_duration_jd_month)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
        gc.collect()