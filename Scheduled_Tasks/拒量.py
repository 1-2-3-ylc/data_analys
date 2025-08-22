
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from apscheduler.triggers.cron import CronTrigger

plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False
from apscheduler.schedulers.background import BackgroundScheduler

import warnings

warnings.filterwarnings("ignore")
import sys
from pathlib import Path
import pymysql
import time
from datetime import timedelta, datetime, timezone
sys.path.append(str(Path(__file__).parent.parent))  # 依实际路径调整

from Class_Model.All_Class import All_Model, Week_Model, Data_Clean


class Rejected_Number:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        # 实例化All_Model类
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()

    # 查询数据
    def select_data(self, month):
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
        , tor.update_time, tomt.reason, cc.channel_type_id, om.order_type
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
        and date_format(om.create_time, '%Y-%m') = '{month}';
        '''
        sql3 = ''' -- 拒量拒绝原因
        SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status  
        FROM `db_credit`.risk_examine
        '''

        sql_risk = ''' -- risk等级
        select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r 

        , replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.union_rent_tag') end,'"','') as union_rent_tag
        , replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.union_rent_rejected') end,'"','') as union_rent_rejected 
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
        f_path_ck = "F:/myfile/p站数据/台账数据/维客壹佰2025年台账.xlsx"
        df_ck = pd.read_excel(f_path_ck, sheet_name="2025")
        df_order = self.clean.query(sql1)
        df_order = df_order[df_order.type != 4]
        df_risk_examine = self.clean.query(sql3)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)
        # 分配人数据
        sql_name = '''
        SELECT tuvor.order_id, tu.nick_name 分配人, tuvor.create_time create_time_t
        FROM db_digua_business.t_user_verify_order_record tuvor
        left join db_digua_business.t_user tu on tuvor.user_id = tu.id 
        where tuvor.del_flag = 0 ORDER BY tuvor.create_time
        '''
        df_name = self.clean.query(sql_name)
        # 资方订单分期表
        sql_stages = '''
        select 
        tos.order_id, tos.sort, tos.refund_date, tos.reality_refund_date
        from db_digua_business.t_order_stages tos
        where tos.sort = 2
        '''
        df_stages = self.clean.query(sql_stages)
        return df_order, df_risk_examine, df_ck, df_risk, df_re, df_ra, df_name, df_stages

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
        df.loc[:, "优惠券使用否"] = np.where(
            (df["new_actual_money"] - df["all_money"] > 0) & (df["租赁方案"] == '租完即送'), "已使用", "未使用")
        df.loc[:, "是否进行预授权"] = np.where(df["total_freeze_fund_amount"].isnull(), "未预授权", "已预授权")

        df.loc[:, "颜色"] = df.apply(lambda x: self.clean.getcolor(x["sku_attributes"]), axis=1)
        df.loc[:, "内存"] = df.apply(lambda x: self.clean.getneicun(x["sku_attributes"]), axis=1)
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'],
                                            x['order_type']), axis=1)
        # 订单去重
        df = self.clean.order_drop_duplicates(df)
        # 定义状态
        df = df.merge(df_risk[['trace_id', 'status_r', 'union_rent_tag', 'union_rent_rejected']], on='trace_id',
                      how='left').merge(
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

    # 获取拒量（回捞）数据
    def get_data(self, df, df_name):
        df_jl_new = df[df.tips.str.contains(r'策略2412|命中自有模型回捞策略|回捞策略250330命中') == True]
        df_jl_new = df_jl_new[~df_jl_new.merchant_name.isin(['小蚂蚁租机', '人人享租', '崇胜数码', '喜卓灵租机', '兴鑫兴通讯', '喜卓灵新租机'])]
        df_jl_new['策略命中等级'] = df_jl_new['tips'].str.extract(r'(策略241205命中\(\d+\)?|策略241212命中\(\d+\)?|命中自有模型回捞策略|回捞策略250330命中?)')[0]
        # 进件数，出库数，出库率，风险等级
        df_jl_new_group = df_jl_new.groupby('下单日期').agg({'是否进件': 'sum', '是否出库': 'sum'})
        df_jl_new_group.rename(columns={'是否进件': '进件数', '是否出库': '出库'}, inplace=True)
        df_jl_new_group['进件出库率'] = (df_jl_new_group.出库 / df_jl_new_group.进件数).map(lambda x: format(x, '.2%'))
        df_jl_new2 = df_jl_new[['下单日期', 'order_number', '策略命中等级']]
        df_jl_new2_group = df_jl_new2.groupby('策略命中等级').agg(数量=('order_number', 'count'))

        # 获取拒量订单的分配人
        df_name_new = df_name.sort_values('create_time_t', ascending=False).groupby('order_id').head(1)
        df_jl_name = df_jl_new[df_jl_new.是否出库==1].merge(df_name_new, on='order_id', how='left')
        df_jl_name_new = df_jl_name[['下单日期', 'order_id', 'order_number', '策略命中等级', '分配人']]
        # 返回拒量数据、拒量中按下单日期分组的进件出库数据、拒量中按策略命中等级分组的订单数量、包含拒量订单分配人的数据
        return df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new

    # 获取首逾数据
    def sort_2(self, df, df_stages, df_name_new):
        df_new = df[df.是否出库==1]
        df_stages_new = df_stages[df_stages['order_id'].notna()]
        df_stages_new['order_id'] = df_stages_new['order_id'].astype(int)
        df_stages_new.loc[:, '预计还款月份'] = df_stages_new.refund_date.dt.strftime('%Y-%m')
        df_new_merge = df_new.merge(df_stages_new, on='order_id', how='left')
        df_jl_stages_new = df_new_merge[(df_new_merge.sort == 2)]
        df_jl_stages_new.loc[:, 'now_day'] = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
        df_jl_stages_new.loc[:, 'reality_refund_date'] = pd.to_datetime(df_jl_stages_new.reality_refund_date.dt.strftime('%Y-%m-%d'))
        # 逾期天数：实还日期不为空且实还日期大于应还日期，用实还日期-应还日期；否则应还日期小于当前日期，用当前日期-应还日期
        df_jl_stages_new.loc[:, 'overdue_day'] = np.where(df_jl_stages_new.reality_refund_date.notna(),
                                                        np.where(df_jl_stages_new.reality_refund_date > df_jl_stages_new.refund_date,
                                                                (df_jl_stages_new.reality_refund_date - df_jl_stages_new.refund_date),0)
                                                        , np.where(df_jl_stages_new.refund_date > df_jl_stages_new.now_day, 0,
                                                                (df_jl_stages_new.now_day - df_jl_stages_new.refund_date)))
        df_jl_stages_new.loc[:, 'overdue_day'] = df_jl_stages_new['overdue_day'].apply(lambda x: x.days)
        df_jl_stages_news = df_jl_stages_new[['order_id', 'order_number', '下单日期', 'refund_date', 'reality_refund_date', 'overdue_day','status2']].rename(columns={'order_id': '订单id', 'order_number': '订单号', 'refund_date': '应还日期','reality_refund_date': '实还日期', 'overdue_day': '逾期天数'})
        df_jl_stages_news = df_jl_stages_news.merge(df_name_new[['order_id', '分配人']], left_on='订单id',right_on='order_id', how='left')
        df_jl_stages_news.loc[:, '是否逾期'] = np.where((df_jl_stages_news.status2 == '租赁中') & (df_jl_stages_news.实还日期.isna()) & (df_jl_stages_news.逾期天数 > 0), 1, 0)
        df_jl_stages_news_g = df_jl_stages_news.groupby('分配人').agg({'order_id': 'count', '是否逾期': 'sum'}).rename(columns={'order_id': '出库'})
        df_jl_stages_news_g.loc[:, '逾期/出库'] = (df_jl_stages_news_g.是否逾期 / df_jl_stages_news_g.出库).map(lambda x: format(x, '.2%'))

        return df_jl_stages_news, df_jl_stages_news_g


    def run(self, month):
        print('正在获取数据...')
        df_order, df_risk_examine, df_ck, df_risk, df_re, df_ra, df_name,df_stages = self.select_data(month)
        print('数据获取完毕...\n正在清理数据...')
        df_contain, df, df2, dfck, df_j = self.clean_data(df_order, df_ck, df_risk, df_re, df_ra)
        print('数据清理完毕...\n正在获取数据...')
        df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new = self.get_data(df, df_name)
        print('数据获取完毕...')
        return df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new,df_stages

    # 首逾定时任务
    def my_job_sort(self, hour, minute, path):
        # 获取每月第一天
        day = datetime.now().strftime('%d')
        # 判断是不是每个月的1号，如果是则获取上一个月的全部数据
        print('正在获取数据...')
        # 判断当前时间是否是一个月的1-5号（包含端点），如果是，则获取上上个月的数据。
        if day in ['01', '02', '03', '04', '05']:
            month = (datetime.now()-pd.DateOffset(months=2)).strftime('%Y-%m')
            t_date = datetime.now().strftime('%Y%m%d')
            print(f'当前时间是{t_date}的{hour}时{minute}分...')
            df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new,df_stages = self.run(month)
            print('正在获取首逾数据...')
            df_jl_stages_news, df_jl_stages_news_g = self.sort_2(df_jl_new, df_stages, df_jl_name_new)
            print('首逾数据获取完毕！\n正在写入数据...')

            with pd.ExcelWriter(path+f'首逾_{t_date}.xlsx', engine='xlsxwriter') as writer:
                df_jl_stages_news.to_excel(writer, sheet_name='出库订单', index=False)
                df_jl_stages_news_g.to_excel(writer, sheet_name='首逾订单')
            print('数据写入完毕！')

    # 每周一首逾定时任务
    def my_job_monday(self, hour, minute, path):
        # 获取当天的数据
        today = pd.to_datetime(datetime.now().strftime('%Y-%m-%d'))
        # 获取当天是星期几
        weekday = today.day_name()
        # 判断当天日期是否是跨月的周一
        print(f'当前时间是{weekday}的{hour}时{minute}分...')
        # 如果当前日期是当月的前7天内，则获取上上个月的数据，否则上个月
        if int(datetime.now().strftime('%d'))<=7:
            month = (datetime.now() - pd.DateOffset(months=2)).strftime('%Y-%m')
            df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new, df_stages = self.run(month)
        else:
            month = (datetime.now() - pd.DateOffset(months=1)).strftime('%Y-%m')
            df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new, df_stages = self.run(month)
            df_jl_new = df_jl_new[df_jl_new.下单日期<=today]
            df_jl_name_new = df_jl_name_new[df_jl_name_new.下单日期<=today]
        print('正在获取首逾数据...')
        df_jl_stages_news, df_jl_stages_news_g = self.sort_2(df_jl_new, df_stages, df_jl_name_new)
        print('首逾数据获取完毕！\n正在写入数据...')
        t_date = datetime.now().strftime('%Y%m%d')
        with pd.ExcelWriter(path + f'每周一首逾_{t_date}.xlsx', engine='xlsxwriter') as writer:
            df_jl_stages_news.to_excel(writer, sheet_name='出库订单', index=False)
            df_jl_stages_news_g.to_excel(writer, sheet_name='首逾订单')
        print('数据写入完毕！')

    def my_job(self, hour, minute, path):
        # 获取每月的月份和第一天
        month = datetime.now().strftime('%Y-%m')
        day = datetime.now().strftime('%d')
        # 判断是不是每个月的1号，如果是则获取上一个月的全部数据
        if day == '01':
            month = (datetime.now()-pd.DateOffset(months=1)).strftime('%Y-%m')
        Today = datetime.now().strftime('%Y%m%d%H')
        print(f'当前时间是{month}月{day}日的{hour}时{minute}分...')
        df_jl_new, df_jl_new_group, df_jl_new2_group, df_jl_name_new,df_stages = self.run(month)
        print('正在写入数据...')
        with pd.ExcelWriter(path + f'拒量数据_{Today}.xlsx', engine='xlsxwriter') as writer:
            df_jl_new_group.to_excel(writer, sheet_name='转化数据')
            df_jl_new2_group.to_excel(writer, sheet_name='拒量数据明细')
            df_jl_name_new.to_excel(writer, sheet_name='出库单分配人', index=False)
        print('数据写入完毕...')

if __name__ == '__main__':
    hour = 13
    minute = 30
    path = r'\\digua\迪瓜租机\22.拒量数据/'
    rn = Rejected_Number()
    # rn.my_job_monday(9, 15, path)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' 或数字 0-6（0 表示周日，1 表示周一，依此类推）。
    # job1 = scheduler.add_job(rn.my_job, 'cron', day=1, hour=14, minute=15, args=[14, 15, path])
    # 每天的13点30分开始执行
    job = scheduler.add_job(rn.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path]) # day_of_week='mon, thu',
    # 每个月初的1-5号15点01分开始执行
    job_sort = scheduler.add_job(rn.my_job_sort, 'cron', month='*', day='1-5', hour=15, minute=1, args=[15, 1, path])
    # 每周一的9点15分开始执行
    job_monday = scheduler.add_job(rn.my_job_monday, 'cron', day_of_week='mon', hour=9, minute=15, args=[9, 15, path])
    # rn.my_job_sort(15, 1, path)
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()
    # rn.my_job(hour, minute, path)
    # rn.my_job_sort(15, 1, path)
    # rn.my_job_monday(9, 15, path)
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
            next_run_time_monday = job_monday.next_run_time
            next_run_time_sort = job_sort.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            elif next_run_time_monday:
                now = datetime.now(timezone.utc)
                sleep_duration_monday = (next_run_time_monday - now).total_seconds()
                if sleep_duration_monday > 0:
                    time.sleep(sleep_duration_monday)
            elif next_run_time_sort:
                now = datetime.now(timezone.utc)
                sleep_duration_sort = (next_run_time_sort - now).total_seconds()
                if sleep_duration_sort > 0:
                    time.sleep(sleep_duration_sort)
            else:
                time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
