import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
import numpy as np
from sqlalchemy.dialects.mssql.information_schema import columns

plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
import gc
import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
from datetime import timedelta ,datetime, timezone
import time
from dateutil.relativedelta import relativedelta
import sys
from pathlib import Path

# 将项目根目录添加到 sys.path
sys.path.append(str(Path(__file__).parent.parent))  # 依实际路径调整
from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class Product_Id:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()

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
                ,om.order_type, tor.update_time, tomt.reason, tpmn.name 机型, cc.scene, tp.id 商品_ID, tp.name phone_name
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
                and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -7 day )
                and om.create_time < CURDATE()
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
        # df_order.loc[:, 'phone_name'] = df_order.phone_name.str.replace(' ', '').str.extract(r'(iPhone\d+(ProMax|Pro|Plus)?)')[0]
        df_order = df_order.merge(df_upv, left_on=['scene', 'create_date'], right_on=['scene', 'c_date'], how='left')
        # df_order.loc[:,'商品ID'] = df_order.商品_ID.astype(str)+'_'+df_order.phone_name
        df_order.loc[:, '机型'] = np.where(df_order.机型.notna(), df_order.机型, df_order.phone_name)

        df_order.loc[:, '商品ID'] = df_order.商品_ID.astype(str)+'_'+df_order.机型


        return df_order, df_risk, df_risk_examine, df_re, df_ra
    # 不去重
    def clean_data_bqc(self, df, df_risk, df_re, df_ra, key=None):
        # 日期处理
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        df["下单月份"] = df["create_time"].dt.strftime('%Y-%m')
        df['年份'] = df["下单日期"].dt.year
        df['hour'] = df['create_time'].dt.hour
        print(df.下单日期.unique())
        # 备注信息处理
        df['拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        df = df[df['sku_attributes'].notnull()]
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()

        df.loc[:, "内存"] = df.apply(lambda x: self.clean.getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, '机型内存'] = df.机型+'_'+df.内存.fillna('未知内存')
        df.loc[:, "商品类型"] = np.where(
            df["product_name"].str.contains('99新') | df["product_name"].str.contains('95新') | df[
                "product_name"].str.contains('准新'), "二手", "全新")


        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"],
                                            x['channel_type_id'], x['order_type']), axis=1)
        # 京东不去重数据，不需要执行订单去重处理
        # if key != '京东':
        #     # 订单去重处理
        #     df = self.clean.order_drop_duplicates(df)
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
        # 曙光计划和线下小店是同分异构体
        df.drop(df[df['activity_name'] == "1000单秘密计划"].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单秘密计划-无优惠"].index, inplace=True)
        df.drop(df[df['activity_name'] == "1000单曙光计划"].index, inplace=True)
        df.drop(df[df['activity_name'] == "线下门店3个月试行"].index, inplace=True)
        # 删除订单状态空值行
        df.dropna(subset=["status2"], axis=0, inplace=True)
        df.drop(df[df['merchant_name'] == "线下小店"].index, inplace=True)

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
        # df = self.clean.drop_merchant(df)
        # 获取节点状态数据
        df = self.clean.status_node(df)

        # 剔除拒收数据
        # df = df[df['物流状态'] != 5]
        # 剔除据量数据
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        df2 = df2[df2.是否拒量 == 0]



        return df, df2

    def my_job(self,hour, minute, path):
        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        df_all, df_risk, df_risk_examine, df_re, df_ra = self.select_data()
        df, df2 = self.clean_data_bqc(df_all, df_risk, df_re, df_ra)
        print('数据清理完毕！\n获取支付宝订单数据...')


        df = df[df.order_method==1]
        df2 = df2[df2.order_method==1]
        df_zm = df[df['归属渠道']=='芝麻租物']
        df_zm2 = df2[df2['归属渠道']=='芝麻租物']
        df_zm_group = self.all_models.data_group(df_zm, df_zm2, df_risk_examine, ['商品ID', 'product_name', '商品类型'])

        df_zm_group = df_zm_group[["去重订单数","进件数",'出库','进件出库率']].fillna(0)
        df_zm_group_new = df_zm_group.reset_index()

        with pd.ExcelWriter(path + f'租物商品ID转化不去重_{today}.xlsx') as writer:
            df_zm_group_new.to_excel(writer, sheet_name='租物商品ID转化不去重', index=False)


if __name__ == '__main__':
    PI = Product_Id()
    hour = 9
    minute = 25
    path = r'\\digua\迪瓜租机\002数据监测\3.转化数据\租物商品ID转化/'

    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天18点10分开始执行，获取每天18点前的数据
    job = scheduler.add_job(PI.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path])
    PI.my_job(hour, minute, path)
    print('定时任务创建完毕...\n正在执行定时任务...')
    # 查看是否添加了任务
    scheduler.start()

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


