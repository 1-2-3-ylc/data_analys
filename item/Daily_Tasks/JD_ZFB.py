from operator import index

import pandas as pd
import numpy as np
import pymysql
import warnings

from apscheduler.triggers.cron import CronTrigger
from dateutil.utils import today

warnings.filterwarnings('ignore')

from datetime import datetime, timedelta, timezone
import time
from apscheduler.schedulers.background import BackgroundScheduler

from Class_Model.All_Class import All_Model, Data_Clean, Week_Model

class JD_ZFB_Data:
    def __init__(self):
        self.all_model = All_Model()
        self.clean = Data_Clean()
        self.week_model = Week_Model()
        # 替换为你的 Webhook 地址
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=d4072f19c1ebe08ea7a71a22df26337eb2fb51327c0ffeac14f8b53b4ed29c78"
        # 替换为你的密钥，如果没有设置则留空
        self.secret = "SEC953fc60a7f3cec15501e044bbe0f93d3bcbb5d68cb6628599f6a0eff94a2a6d4"


    # 查询数据
    def select_data(self, date):
        sql1 = f''' -- 订单&风控信息  近10日数据   
                SELECT om.create_time,om.id as order_id ,om.order_number,om.status, date(om.create_time) as create_date
                ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
                when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
                when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as status2 
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
                ,om.order_method, om.activity_id, tpmn.name 机型
                , om.order_type, tor.update_time, tomt.reason, tprm.purchase_amount, tprm.all_deposit, tprm.all_rental, cc.scene 
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
                where om.user_mobile is not null 
                and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
                ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
                and  date_format(om.create_time, '%Y-%m-%d')>='{date}'
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
        tsc.scene, date(tsc.day) c_date, tsc.new_uv_count, tsc.uv_count
        from db_digua_business.t_statistics_channel tsc
        '''
        df_upv = self.clean.query(sql_upv)
        df_order = df_order.merge(df_upv, left_on=['scene', 'create_date'], right_on=['scene', 'c_date'], how='left')

        return df_order, df_risk, df_risk_examine, df_re, df_ra

    # 数据清理
    def clean_data(self, df, df_risk, df_re, df_ra, key=None):
        # 日期处理
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        df["下单月份"] = df["create_time"].dt.strftime('%Y-%m')
        df['年份'] = df["下单日期"].dt.year
        df['hour'] = df['create_time'].dt.hour

        # 备注信息处理
        df['拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        df = df[df['sku_attributes'].notnull()]
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()

        df.loc[:, "内存"] = df.apply(lambda x: self.clean.getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"],
                                            x['channel_type_id'], x['order_type']), axis=1)
        if key != '京东':
            # 订单去重处理
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
            # 删除身份证空值行
            df.dropna(subset=["status2"], axis=0, inplace=True)
            # 删除重复订单
            df.drop_duplicates(subset=["order_id"], inplace=True)
            df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)
            df.drop(df[df['true_name'].isin(
                ["刘鹏", "谢仕程", "潘立", "洪柳", "陈锦奇", "周杰", "卢腾标", "孔靖", "黄娟", "钟福荣", "邱锐杰", "唐林华"
                    , "邓媛斤", "黄子南", "刘莎莎", "赖瑞彤", "孙子文"])].index, inplace=True)

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
        # 剔除据量数据
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        df2 = df2[df2.是否拒量 == 0]
        return df, df2

    def get_jd(self, df, df2, df_risk_examine, path):
        today = datetime.now().strftime('%Y%m%d')
        df = df[df.归属渠道=='京东渠道']
        df2 = df2[df2.归属渠道=='京东渠道']
        df_jd_group = self.all_model.data_group(df, df2, df_risk_examine, '下单日期')
        df.loc[:, 'uv'] = np.where(df.uv_count==0, df.new_uv_count, df.uv_count)
        df_jd_uv = df.groupby('下单日期').agg({'uv': 'count'})
        df_jd = df_jd_uv.merge(df_jd_group, on='下单日期', how='left')
        df_jd.index= df_jd.index.astype(str)
        df_jd = df_jd[['uv', "去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
            "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系", "出库前风控强拒", "待审核",
            '出库', '进件出库率', '取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率']].fillna(0)
        with pd.ExcelWriter(path + f'京东转化数据_{today}.xlsx', engine='xlsxwriter') as writer:
            df_jd.to_excel(writer, sheet_name='京东转化')

    def jd_zfb(self, df, path):
        today = datetime.now().strftime('%Y%m%d')
        df_jd = df[df.归属渠道=='京东渠道']
        df_zfb = df[df.order_type=='ZFB_ORDER']
        def get_data(df2):
            df2 = df2[(df2.purchase_amount>0)&(df2.是否出库==1)]
            df2_group = df2.groupby('下单日期').agg({'purchase_amount': 'sum', 'new_actual_money': 'sum', 'all_deposit': 'mean', 'all_rental': 'mean'}).rename(columns={'purchase_amount': '采购金额', 'new_actual_money': '买断价', 'all_deposit': '总押金均值', 'all_rental': '总租金均值'})
            df2_group = self.week_model.W_group(self.week_model.custom_weekly_resampler(df2_group, 7))
            df2_group.loc[:, '毛利率'] = ((df2_group.买断价-df2_group.采购金额)/df2_group.采购金额).map(lambda x: format(x, '.2%'))
            df2_group.loc[:, '租售比'] = (df2_group.总租金均值/df2_group.总押金均值).map(lambda x: format(x, '.2%'))
            return df2_group[['week_group', '采购金额', '买断价', '毛利率', '总租金均值', '总押金均值', '租售比']]
        df_jd_data = get_data(df_jd)
        df_zfb_data = get_data(df_zfb)
        with pd.ExcelWriter(path + f'毛利率_{today}.xlsx', engine='xlsxwriter') as writer:
            df_jd_data.to_excel(writer, sheet_name='京东', index=False)
            df_zfb_data.to_excel(writer, sheet_name='支付宝', index=False)

    def get_model(self, df, path):
        df = df[(df.下单日期>='2025-06-16')&(df.下单日期<='2025-06-22')&(df.归属渠道=='京东渠道')&(df.是否出库==1)&(df.purchase_amount>0)]
        df_group = df.groupby('机型').agg({'order_number': 'count', 'all_rental': 'mean', 'all_deposit': 'mean'}).rename(columns={'order_number': '数量', 'all_rental': '总租金均值', 'all_deposit': '总押金均值'})
        df_group.to_excel(path+'京东渠道机型转化.xlsx')

    def run(self, hour, minute, path):
        print(f'执行定时任务：现在是每日的{hour}点{minute}分...')
        date = '2025-06-02'
        print('正在查询数据...')
        df_order, df_risk, df_risk_examine, df_re, df_ra = self.select_data(date)
        print('数据查询完毕！\n正在清理数据...')
        df, df2 = self.clean_data(df_order, df_risk, df_re, df_ra)
        print('数据清理完毕！\n正在获取京东数据...')
        self.get_jd(df, df2, df_risk_examine, path)
        print('京东数据获取完毕！\n正在获取京东和支付宝金额数据...')
        self.jd_zfb(df2, path)
        print('京东和支付宝金额数据获取完毕！')
        # self.get_model(df2, path)


if __name__ == '__main__':
    hour = 8
    minute = 30
    path = r'\\digua\迪瓜租机\002数据监测\7.周数据/'
    jzd = JD_ZFB_Data()
    # jzd.run(hour, minute, path)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(jzd.run, CronTrigger(day_of_week='mon', hour=hour, minute=minute), args=[hour, minute, path])
    print('定时任务创建完毕...\n正在执行定时任务...')
    # 查看是否添加了任务
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