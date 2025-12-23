import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
# import seaborn as sns
#sns.set(style="darkgrid")
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False


from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import time

import warnings
warnings.filterwarnings("ignore")

import sys
import pymysql
from sqlalchemy import create_engine
import json
import datetime
from datetime import timedelta ,timezone, datetime

from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

from apscheduler.schedulers.background import BackgroundScheduler

class Channel_Analysis:
    def __init__(self, channel_name):
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()
        self.channel_name = channel_name

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

    # 获取数据
    def select_data(self):
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
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.is_vip') end,'"','') as is_vip
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.status') end,'"','') as status_result
        ,replace(case when JSON_VALID(tor.risk) THEN JSON_EXTRACT(tor.risk, '$.queue_verify_type') end,'"','') as qvt_risk
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.queue_verify_type') end,'"','') as qvt_result
        ,cc.name as channel_name         -- 来源渠道
        ,cc.channel_type_id
        ,pa.name as activity_name        -- 活动名称
        ,om.merchant_id,om.merchant_name
        ,topay.total_freeze_fund_amount 
        ,om.buy_service_product,tso.status as service_status 
        ,om.order_method,tomt.reason, order_type
        from  db_digua_business.t_order  om 
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
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静'
        ,'陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        -- and COALESCE(pa.name, '未知') not in ("1000单秘密计划","1000单秘密计划-无优惠","1000单曙光计划","线下门店3个月试行") 
        and  om.create_time >= DATE_ADD(CURRENT_DATE, INTERVAL -1 month) 
        -- and  om.create_time <= DATE_ADD(CURRENT_DATE, INTERVAL -12 day)        -- 整月数据
        ;
        '''
        sql_risk = ''' -- risk等级
        select trace_id, id_card as id_card_r, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status_r from db_credit.risk
        '''
        sql3 = '''
        SELECT risk_trace_id trace_id, id_card, time, replace(case when JSON_VALID(data) THEN JSON_EXTRACT(data, '$.status') end,'"','') as status   FROM `db_credit`.risk_examine
        '''
        df_risk_examine = self.query(sql3)
        df_re = df_risk_examine.copy()
        df_re.loc[:, 'time_re'] = pd.to_datetime(df_re.time.dt.date, errors="coerce")
        df_re.rename(columns={'id_card': 'id_card_re', 'status': 'status_re'}, inplace=True)
        df_risk = self.query(sql_risk)
        sql_ra = ''' -- 996强拒表
        select order_id, time, replace(case when JSON_VALID(result) THEN JSON_EXTRACT(result, '$.status') end,'"','') as status_ra  from db_credit.risk_alipay_interactive_prod_result
        '''
        df_ra = self.query(sql_ra)
        df_ra.loc[:, 'time_ra'] = pd.to_datetime(df_ra.time.dt.date, errors="coerce")
        df_ra = df_ra.sort_values(by='time', ascending=False).groupby('order_id').head(1)
        df = self.query(sql1)

        return df, df_risk, df_re, df_ra


    # 清理数据
    def clean_data(self, df, df_risk, df_re, df_ra):
        # 处理日期
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["月份"] = df["下单日期"].dt.month
        df['create_hour'] = df["create_time"].dt.hour
        # 处理原因
        df['拒绝理由'] = df["rejected"].str.replace("[", "").str.replace("]", "").str.replace('"', '')
        df = df[df['sku_attributes'].notnull()]
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].astype(str).str.split("$").str[
            0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].astype(str).str.split("$").str[
            0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].astype(str).str.split("$").str[
            0].str.strip()
        # 处理方案
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
        df.loc[:, "颜色"] = df.apply(lambda x: getcolor(x["sku_attributes"]), axis=1)
        # 获取内存
        def getneicun(s):
            color_list = json.loads(s)
            for j in range(0, len(color_list)):
                if color_list[j]["key"] == "内存":
                    return color_list[j]["value"]
        df.loc[:, "内存"] = df.apply(lambda x: getneicun(x["sku_attributes"]), axis=1)
        # 判断进件
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        # 判断来源渠道
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        df.loc[:, "归属渠道"] = df.apply(lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'], x['order_type']), axis=1)
        # 判断免审
        df['免审'] = np.where(df.decision_result.str.contains(pat='免人审', regex=False), 1, 0)
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
        # 删除身份证空值行
        df.dropna(subset=["status2"], axis=0, inplace=True)

        # 删除重复订单
        df.drop_duplicates(subset=["order_id"], inplace=True)
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)

        df.drop(df[df['true_name'].isin(
            ["刘鹏", "谢仕程", "潘立", "洪柳", "陈锦奇", "周杰", "卢腾标", "孔靖", "黄娟", "钟福荣", "邱锐杰", "唐林华"
                , "邓媛斤", "黄子南", "刘莎莎", "赖瑞彤", "孙子文"])].index, inplace=True)
        # 定义状态
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
                                  x["无法联系原因"], x["total_describes"], x['是否前置拦截'], x['是否机审强拒'],
                                  x['是否出库前风控强拒']), axis=1)
        self.df_jj = df[df.进件=='进件']
        # 剔除商家
        df = self.clean.drop_merchant(df)
        # 获取进件订单
        df["下单时段"] = df["create_time"].astype(str).str[:14]
        df_j = df[df["进件"] == "进件"]
        # 获取各个节点的状态
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

        return df

    # 导入台账
    def get_tz_data(self, path, df):
        # 获取台账数据
        df_ck = pd.read_excel(path, sheet_name="2025")
        dfck = pd.merge(df_ck, df, left_on="订单号", right_on="order_number")

        dfck.drop_duplicates(subset=["订单号"], inplace=True)
        # 剔除已退款，露营设备
        dfck.drop(dfck[dfck["status2"] == "已退款"].index, inplace=True)
        try:
            dfck.drop(dfck[dfck["类目"] == "露营设备"].index, inplace=True)
        except:
            dfck.drop(dfck[dfck["类型"] == "露营设备"].index, inplace=True)
        return dfck

    # 计算环比
    def calculate_growth_rate(self, current, previous):
        if previous == 0:
            return float('inf') if current > 0 else float('-inf')
        return (current - previous) / previous


    # 按天获取进件数据
    def df_all_day(self, date, channel_name, hour, minute, start_day, end_day=0):
        # 每日各渠道去重订单统计结果表格导出
        qd_list = ['create_time', '付费灯火', '八派信息', '九州信息', '单人聊天会话中的小程序消息卡片（分享）', '我的小程序入口',
                    '搜索渠道', '支付宝社群', '生活号', '芝麻租物', '小程序商家消息（服务提醒）', '支付宝 push 消息（同1014）', '派金花', '支付宝直播', '京东渠道']
        gsqd_jj = pd.crosstab(date, channel_name, margins=True)

        qd_names = channel_name.drop_duplicates()[channel_name.drop_duplicates().isin(qd_list)].to_list()
        qd_namess = channel_name.drop_duplicates()[channel_name.drop_duplicates().isin(qd_list)].to_list()
        qd_namess.append('create_time')

        gsqd_jj = gsqd_jj.reset_index()
        gsqd_jj = gsqd_jj[qd_namess][:-1]
        gsqd_jj['create_time'] = pd.to_datetime(gsqd_jj['create_time'])
        gsqd_jj['日期'] = gsqd_jj['create_time'].dt.date
        gsqd_jj['小时'] = gsqd_jj['create_time'].dt.hour

        # 定义截止时间到小时分钟
        gsqd_jj['下单时间'] = pd.to_datetime(gsqd_jj['create_time'])
        gsqd_jj['下单日期'] = gsqd_jj['下单时间'].dt.date

        # 定义时间阈值
        time_threshold = pd.to_datetime(f'{hour}:{minute}').time()
        # 按 '下单日期' 分组
        gsqd_jj = gsqd_jj.groupby('下单日期')
        # 初始化一个空的 DataFrame 来存储结果
        result_df = pd.DataFrame()
        # 遍历每个组并筛选小于 15:30 的数据
        for date, group in gsqd_jj:
            filtered_group = group[group['下单时间'].dt.time <= time_threshold]
            result_df = pd.concat([result_df, filtered_group], ignore_index=True)

        gsqd_jj = result_df.copy()
        gsqd_jj_group = gsqd_jj.groupby(['下单日期', '小时'])[qd_names].sum()

        # 截止日期
        gsqd_jj_group = gsqd_jj_group.reset_index()
        gsqd_jj_group['下单日期'] = pd.to_datetime(gsqd_jj_group['下单日期'])
        three_ago = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - timedelta(days=start_day)
        three_after = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')) - timedelta(days=end_day)
        if end_day==0:
            gsqd_jj_group = gsqd_jj_group[gsqd_jj_group['下单日期'] >= three_ago]
        else:
            gsqd_jj_group = gsqd_jj_group[(gsqd_jj_group['下单日期']>=three_ago)&(gsqd_jj_group['下单日期']<=three_after)]
        # 获取规定小时之内的数据
        # gsqd_jj_group = gsqd_jj_group.query(f'小时<{hour}')
        # 对获取的数据进行累计并取累计之后的值
        gsqd_jj_group.set_index('下单日期', inplace=True)
        gsqd_jj_groups = gsqd_jj_group.groupby(['下单日期'])[qd_names].cumsum()
        gsqd_jj_groups = gsqd_jj_groups.groupby(['下单日期'])[qd_names].last()
        gsqd_jj_groups.insert(0, '小时', hour)
        gsqd_jj_groups = gsqd_jj_groups.reset_index()
        # 获取日期
        try:
            first = gsqd_jj_groups['下单日期'].iloc[0].strftime('%m%d')
            current = gsqd_jj_groups['下单日期'].iloc[-1].strftime('%m%d')
            previous = gsqd_jj_groups['下单日期'].iloc[-2].strftime('%m%d')
        except:
            first = '00:00'
            current = '00:00'
            previous = '00:00'

        # 创建新的df对象存储数据
        gsqd_jjs = pd.DataFrame({
            '下单日期': [f'{current}与{previous}对比增长数量', f'{current}与{previous}对比增长率'],
            '小时': [' ', ' ']
        })
        for qd in qd_names:
            try:
                gsqd_jjs[qd] = [gsqd_jj_groups[qd].diff().iloc[-1],
                                (gsqd_jj_groups[qd].pct_change() * 100).apply(lambda x: f'{x:.0f}%').iloc[-1]]
            except:
                gsqd_jjs[qd] = [0, 0]
        gsqd_jjs = pd.concat([gsqd_jj_groups, gsqd_jjs])
        # 获取df的列名并固定排序
        qd_lists = ['下单日期', '小时', '芝麻租物', '搜索渠道', '付费灯火', '八派信息', '九州信息', '单人聊天会话中的小程序消息卡片（分享）',
                   '我的小程序入口', '支付宝社群', '生活号', '小程序商家消息（服务提醒）',
                   '支付宝 push 消息（同1014）', '派金花', '支付宝直播', '京东渠道']
        col_list = [col for col in qd_lists if col in gsqd_jj_group.columns]
        col_list2 = [col for col in qd_lists if col in gsqd_jjs.columns]
        return gsqd_jj_group[col_list], gsqd_jjs[col_list2], first, current


    # 获取数据
    def get_data(self, f_path_ck, df, chennel_name, hour, minute, start_day, end_day=0):
        # 获取出库数据
        dfck = self.get_tz_data(f_path_ck, df)
        # 获取进件
        df_jj = df[df.是否进件==1]
        # 获取总体数据
        gsqd_group, gsqd, first, current = self.df_all_day(df['create_time'], df[chennel_name], hour, minute, start_day, end_day)
        # 获取出库数据
        gsqd_group_ck, gsqd_ck, first, current = self.df_all_day(dfck['create_time'], dfck[chennel_name], hour, minute, start_day, end_day)
        # 获取进件数据
        gsqd_group_jj, gsqd_jj, first, current = self.df_all_day(df_jj['create_time'], df_jj[chennel_name], hour, minute, start_day, end_day)

        return gsqd_group, gsqd, gsqd_group_ck, gsqd_ck, gsqd_group_jj, gsqd_jj, first, current



    def run(self):
        f_path_ck = "F:/myfile/p站数据/台账数据/维客壹佰2025年台账.xlsx"
        hour = 18
        minute = '00'

        # hour = 13
        # minute = '00'

        print('正在查询数据...')
        df, df_risk, df_re, df_ra = self.select_data()
        print('数据查询完毕...\n正在清理数据...')
        df1 = self.clean_data(df, df_risk, df_re, df_ra)
        print('数据清理完毕...\n正在获取数据...')
        gsqd_group, gsqd, gsqd_group_ck, gsqd_ck, gsqd_group_jj, gsqd_jj, first, current = self.get_data(f_path_ck, df1, self.channel_name, hour, minute, 14)
        print('数据获取完毕...')
        return gsqd_group, gsqd, gsqd_group_ck, gsqd_ck, gsqd_group_jj, gsqd_jj, first, current

    # 定时任务
    def my_job(self, hour, minute, path):
        print(f'执行定时任务：现在是每日的{hour}点{minute}分')
        gsqd_group, gsqd, gsqd_group_ck, gsqd_ck, gsqd_group_jj, gsqd_jj, first, current = self.run()
        print('正在写入数据...')
        with pd.ExcelWriter(path + f'各渠道增长对比分析_{first}-{current}.xlsx', engine='openpyxl') as writer:
            gsqd.to_excel(writer, sheet_name=f'{self.channel_name}去重订单统计(汇总)', index=False)

        with pd.ExcelWriter(path + f'各渠道增长对比分析_{first}-{current}.xlsx', engine='openpyxl', mode='a') as writer:
            gsqd_jj.to_excel(writer, sheet_name=f'{self.channel_name}进件统计(汇总)', index=False)
            gsqd_ck.to_excel(writer, sheet_name=f'{self.channel_name}出库统计(汇总)', index=False)
            gsqd_group.to_excel(writer, sheet_name=f'{self.channel_name}去重订单统计')
            gsqd_group_jj.to_excel(writer, sheet_name=f'{self.channel_name}进件统计')
            gsqd_group_ck.to_excel(writer, sheet_name=f'{self.channel_name}出库统计')
        print('写入数据已完成...')


if __name__ == '__main__':
    hour = 18
    minute = 20

    # hour = 13
    # minute = 39

    path = r'\\digua\迪瓜租机\19.小程序发货率/'
    channel_name = '归属渠道'
    ca = Channel_Analysis(channel_name)
    # ca.run()
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    job = scheduler.add_job(ca.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
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