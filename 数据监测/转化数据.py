from operator import index

import pandas as pd
import numpy as np
import pymysql
import warnings
import gc
from apscheduler.triggers.cron import CronTrigger
from dateutil.utils import today

warnings.filterwarnings('ignore')

from datetime import datetime, timedelta, timezone
import time
from apscheduler.schedulers.background import BackgroundScheduler
from Class_Model.All_Class import All_Model, Data_Clean

class Conversion_Data:
    def __init__(self):
        self.all_model = All_Model()
        self.clean = Data_Clean()
        # 替换为你的 Webhook 地址
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=d4072f19c1ebe08ea7a71a22df26337eb2fb51327c0ffeac14f8b53b4ed29c78"
        # 替换为你的密钥，如果没有设置则留空
        self.secret = "SEC953fc60a7f3cec15501e044bbe0f93d3bcbb5d68cb6628599f6a0eff94a2a6d4"


    # 查询数据
    def select_data(self, date, hour):
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
                and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
                ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
                and  date_format(om.create_time, '%Y-%m-%d')>='{date}'
                and hour(om.create_time)<'{hour}'
                -- and date_format(om.create_time, '%Y-%m-%d')>='2025-08-01'
                -- and date_format(om.create_time, '%Y-%m-%d')<='2025-08-31'
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
        df_order.loc[:,'商品ID'] = df_order.商品ID.astype(str)+'_'+df_order.phone_name


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
        df.loc[:, '机型内存'] = df.机型+'_'+df.内存
        df.loc[:, "进件"] = np.where((df["status2"] == "待支付") | (df["status2"] == "订单取消"), "未进件", "进件")
        df.loc[:, "来源渠道"] = df["channel_name"].fillna("未知渠道")
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"],
                                            x['channel_type_id'], x['order_type']), axis=1)
        # 京东不去重数据，不需要执行订单去重处理
        if key != '京东':
            # 订单去重处理
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

    # 机型转化
    def conversion_model(self, df, df2, df_risk_examine, model):
        today = datetime.now().strftime('%Y-%m-%d')
        # 排除二手手机
        df = df[df.是否二手==0]
        df2 = df2[df2.是否二手==0]
        # 获取机型的转化数据和uv数据
        def group(df1, df12):
            # 调用data_group函数，计算转化数据
            df_model_group = self.all_model.data_group(df1, df12, df_risk_examine, model)
            df_model_group.loc[:, '出库'] = df_model_group.出库+df_model_group.拒量出库
            df_model_group = df_model_group[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件", "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
                "出库前风控强拒", "待审核", '出库',  '总体进件出库率（含拒量）','取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率']].fillna(0)
            # 判断是否是计算商品ID的转化数据，如不是则不需要添加uv数据
            if model == '商品ID':
                df1.loc[:, 'uv'] = np.where(df1.uv_count == 0, df1.new_uv_count, df1.uv_count)
                df1 = df1.drop_duplicates(subset=['order_id',"商品ID", "uv"])
                df_uv = df1.groupby('商品ID').agg({'uv': 'sum'})
                df_model_group = df_uv.merge(df_model_group, on=model, how='left')
            # 根据去重订单数进行倒序排序
            df_model_group = df_model_group.sort_values(by='去重订单数', ascending=False)
            return df_model_group
        # 当天
        df_now = df[df.下单日期==today]
        df2_now = df2[df2.下单日期==today]
        df_model_group_now = group(df_now, df2_now)
        # 近7天
        df_7 = df[df.下单日期>=(datetime.now()-timedelta(days=7))]
        df2_7 = df2[df2.下单日期>=(datetime.now()-timedelta(days=7))]
        df_model_group_7 = group(df_7, df2_7)
        # 近15天
        df_model_group_15 = group(df, df2)
        return df_model_group_now, df_model_group_7, df_model_group_15



    # 数据异常提醒
    def data_tips(self, df, df2, df_risk_examine, model, model_list, keys):
        today = datetime.now().strftime('%Y-%m-%d')
        # 获取所需数据集
        df = df[(df[f'{model}'].isin(model_list))&(df.下单日期>=(datetime.now()-timedelta(days=2)))]
        df2 = df2[(df2[f'{model}'].isin(model_list))&(df2.下单日期>=(datetime.now()-timedelta(days=2)))]
        # 设置提醒值
        df_qc_num = 0.1
        df_jj_num = 0.05
        df_ck_num = 0.015
        # 获取去重和进件的异常信息
        def get_message(df1, value, func, key, num, col_name):
            message_qc = ''
            # 透视表
            df_qc = pd.pivot_table(df1, values=value, columns=f'{col_name}', index='下单日期', aggfunc=func)
            df_qc_list = df_qc.columns.to_list()
            # 循环到每个列名并获取每个列名是否超出异常值
            for qc in df_qc_list:
                qc_num = df_qc[qc].pct_change().iloc[-1]
                if qc_num < -num:
                    if model=='商品ID':
                        message_qc += f'''{qc}的{key}环比为：{str(round(qc_num * 100, 2)) + '%'}，跌出{str(round(num * 100, 2)) + '%'}，警报！警报！警报！\n'''
                    else:
                        message_qc += f'''{qc}的{key}环比为：{str(round(qc_num * 100, 2)) + '%'}，跌出{str(round(num * 100, 2)) + '%'}，警报！警报！警报！\n'''
            return message_qc
        # 去重订单
        # message_qc = get_message(df, 'order_id', 'count', '去重订单数', df_qc_num)
        # 进件
        message_jj = get_message(df, '是否进件', 'sum', '进件数', df_jj_num, model)
        # 出库
        df_ck_group = self.all_model.data_group(df, df2, df_risk_examine, [f'{model}', '下单日期'])
        df_ck_group.loc[:, '出库率'] = pd.to_numeric(df_ck_group['总体进件出库率（含拒量）'].str.replace('%', '').str.replace('nan', '0')) / 100
        df_ck_group = df_ck_group.reset_index()
        message_ck = ''
        # 循环列名值，获取每个出库商品的异常信息
        for models in model_list:
            ck_num = df_ck_group[df_ck_group[f'{model}']==models].出库率.diff().iloc[-1]
            if ck_num < -df_ck_num:
                if model=='商品ID':
                    message_ck += f'''{models}的出库率环比为：{str(round(ck_num * 100, 2)) + '%'}，跌出{str(round(df_ck_num * 100, 2)) + '%'}，警报！警报！警报！\n'''
                else:
                    message_ck += f'''{models}的出库率环比为：{str(round(ck_num * 100, 2)) + '%'}，跌出{str(round(df_ck_num * 100, 2)) + '%'}，警报！警报！警报！\n'''
        message = f'{keys}\n'+message_jj+message_ck
        # 调用机器人发送信息函数
        self.clean.send_dingtalk_message(self.webhook, self.secret, message)

    def run(self, date, hour):
        df, df_risk, df_risk_examine, df_re, df_ra = self.select_data(date, hour)

        return df, df_risk, df_risk_examine, df_re, df_ra

    def jd_job(self, hour, minute, path, hours):
        date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')# 15
        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        print('正在查询数据...')
        # 如果需要更改时间，还需要改这儿date_add(current_date, interval - 31 day)
        sql_jd = ''' -- 京东外部订单关联表
                    select  tojo.create_time, tojo.order_id, tojo.status from db_digua_business.t_order_jd_out_no tojo
                    where 
                    -- date_format(tojo.create_time, '%Y-%m-%d')>='2025-08-01'
                    -- and date_format(tojo.create_time, '%Y-%m-%d')<='2025-08-31'
                    tojo.create_time>=date_add(current_date, interval - 15 day) and tojo.create_time<current_date
                '''
        df_jd = self.clean.query(sql_jd)

        df_jd.loc[:, '下单日期'] = pd.to_datetime(df_jd.create_time.dt.strftime('%Y-%m-%d'))
        # print(df_jd['下单日期'].unique())
        df, df_risk, df_risk_examine, df_re, df_ra = self.run(date, hours)
        print('数据查询完毕！\n正在清理数据...')
        df, df2 = self.clean_data(df, df_risk, df_re, df_ra, key='京东')
        print('数据清理完毕！\n正在计算数据...')
        # 关联数据
        df_jd_new = df_jd.merge(df[['order_id', 'new_actual_money', 'status2']], on='order_id', how='left')
        # 区分京东渠道并计算进件和风控通过的件数
        df = df[df.归属渠道=='京东渠道']
        df2 = df2[df2.归属渠道=='京东渠道']
        df_group = self.all_model.data_group(df, df2, df_risk_examine, '下单日期')


        # 获取京东的不去重买断价
        # 如果需要更改时间，还需要改这儿date_add(current_date, interval - 31 day)
        # tojr.request_json like '%IN_THE_LEASE%'表已签收，不含拒收
        sql_jd_md = '''
        select distinct o.order_number, od.new_actual_money 买断价, tojr.create_time from db_digua_business.t_order as o 
        left join db_digua_business.t_order_details as od on o.id = od.order_id 
        left join db_digua_business.t_order_jd_request tojr on tojr.order_number=o.order_number
        where 
        -- date_format(tojr.create_time, '%Y-%m-%d')>='2025-08-01'
        -- and date_format(tojr.create_time, '%Y-%m-%d')<='2025-08-31'
        tojr.create_time>=date_add(current_date, interval -15 day)
        and tojr.request_json like '%IN_THE_LEASE%'
        '''
        df_jd_md = self.clean.query(sql_jd_md)
        df_jd_md = df_jd_md.sort_values(by='create_time', ascending=False).groupby('order_number').head(1)
        df_jd_md.loc[:, '同步日期'] = df_jd_md.create_time.dt.strftime('%Y-%m-%d')
        df_jd_md.loc[:, '买断价均值'] = df_jd_md.买断价
        df_jd_md.loc[:, '合计买断价'] = df_jd_md.买断价
        df_jd_md_group = df_jd_md.groupby('同步日期').agg({'买断价均值': 'mean', '合计买断价': 'sum'})
        # 计算京东的去重，待发货，已发货和买断价数据
        df_jd_new.loc[:, '待发货'] = np.where((df_jd_new.status=='TO_SEND_GOODS')& (df_jd_new.status2=='待发货'), 1, 0)
        df_jd_new.loc[:, '已发货'] = np.where(df_jd_new.status.isin(['IN_DELIVERY', 'IN_THE_LEASE']), 1, 0)
        df_jd_new_group = df_jd_new.groupby('下单日期').agg({'order_id': 'count', '待发货': 'sum', '已发货': 'sum'}).rename(columns={'order_id': '创建订单数'})
        # 关联各个节点的数据
        df_jd_group = df_jd_new_group.merge(df_group[['进件数', '风控通过件']], on='下单日期', how='left')
        df_jd_group.loc[:, '审核通过率'] = (df_jd_group.风控通过件/df_jd_group.创建订单数).map(lambda x: format(x, '.2%'))
        df_jd_group.loc[:, '风控通过率'] = (df_jd_group.风控通过件/df_jd_group.进件数).map(lambda x: format(x, '.2%'))
        df_jd_group.loc[:, '发货率'] = (df_jd_group.已发货/df_jd_group.创建订单数).map(lambda x: format(x, '.2%'))
        df_jd_group = df_jd_group[['创建订单数', '进件数', '风控通过件', '审核通过率', '风控通过率', '待发货', '已发货', '发货率']]
        df_jd_group.index = df_jd_group.index.astype(str)
        df_jd_group = pd.concat([df_jd_group, df_jd_md_group], axis=1)
        # print(df_jd_group)
        print('数据计算完毕！\n正在写入数据...')

        with pd.ExcelWriter(path+f'京东转化/京东转化数据_{today}.xlsx', engine='xlsxwriter') as writer:
            df_jd_group.to_excel(writer,sheet_name='京东转化数据')
        print('数据写入完毕！')
        del df_jd_group
        gc.collect()
        print("回收内存执行完毕！\n")




    def my_job(self, hour, minute, path, hours):
        date = (datetime.now()-timedelta(days=15)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        print('正在查询数据...')
        df, df_risk, df_risk_examine, df_re, df_ra = self.run(date, hours)
        print('数据查询完毕！\n正在清理数据...')
        df, df2 = self.clean_data(df, df_risk, df_re, df_ra)
        print('数据清理完毕！\n正在计算机型转化数据...')
        # 总体
        df_model_group_now, df_model_group_7, df_model_group_15 = self.conversion_model(df, df2, df_risk_examine, '机型内存')
        model_list = df_model_group_now.sort_values(by='去重订单数', ascending=False).head(10).index.to_list()
        self.data_tips(df, df2, df_risk_examine, '机型内存', model_list, '总体')
        # 芝麻租物
        df_zm = df[df.归属渠道 == '芝麻租物']
        df2_zm = df2[df2.归属渠道 == '芝麻租物']
        df_model_group_now_zm, df_model_group_7_zm, df_model_group_15_zm = self.conversion_model(df_zm, df2_zm, df_risk_examine, '机型内存')
        model_list_zm = df_model_group_15_zm.sort_values(by='去重订单数', ascending=False).head(10).index.to_list()
        self.data_tips(df_zm, df2_zm, df_risk_examine, '机型内存', model_list_zm, '芝麻租物')
        # 芝麻租物
        df_jd = df[df.归属渠道 == '京东渠道']
        df2_jd = df2[df2.归属渠道 == '京东渠道']
        df_model_group_now_jd, df_model_group_7_jd, df_model_group_15_jd = self.conversion_model(df_jd, df2_jd, df_risk_examine, '机型内存')
        model_list_jd = df_model_group_15_jd.sort_values(by='去重订单数', ascending=False).head(10).index.to_list()
        self.data_tips(df_jd, df2_jd, df_risk_examine, '机型内存', model_list_jd, '京东渠道')
        print('机型转化数据计算完毕！\n正在计算商品ID转化数据...')
        id_list = ['6710_iPhone16', '6711_iPhone16Plus', '6712_iPhone16Pro', '6713_iPhone16ProMax', '7681_iPhone16ProMax', '7682_iPhone16Pro', '7683_iPhone16Plus', '7684_iPhone16', '6752_iPhone16', '6756_iPhone16ProMax', '6757_iPhone16Pro']
        # 芝麻
        df_zm_uv = df[df.商品ID.isin(id_list[:-3])]
        df2_zm_uv = df2[df2.商品ID.isin(id_list[:-3])]
        df_zm_uv, df_zm_uv_7, df_zm_uv_15 = self.conversion_model(df_zm_uv, df2_zm_uv, df_risk_examine, '商品ID')
        # 搜索
        df_ss_uv = df[df.商品ID.isin(id_list[-3:])]
        df2_ss_uv = df2[df2.商品ID.isin(id_list[-3:])]
        df_ss_uv, df_ss_uv_7, df_ss_uv_15 = self.conversion_model(df_ss_uv, df2_ss_uv, df_risk_examine, '商品ID')
        self.data_tips(df, df2, df_risk_examine, '商品ID', id_list, '商品ID_芝麻租物')
        print('商品ID转化数据计算完毕！')

        print('正在写入数据...')
        with pd.ExcelWriter(path+f'机型转化/近15日转化数据_总体_{today}.xlsx', engine='xlsxwriter') as writer:
            df_model_group_now.to_excel(writer,sheet_name='当日机型转化')
            df_model_group_7.to_excel(writer,sheet_name='近7日机型转化')
            df_model_group_15.to_excel(writer,sheet_name='近15日机型转化')
        with pd.ExcelWriter(path + f'机型转化/近15日转化数据_芝麻租物_{today}.xlsx', engine='xlsxwriter') as writer:
            df_model_group_now_zm.to_excel(writer, sheet_name='当日机型转化')
            df_model_group_7_zm.to_excel(writer, sheet_name='近7日机型转化')
            df_model_group_15_zm.to_excel(writer, sheet_name='近15日机型转化')
        with pd.ExcelWriter(path + f'机型转化/近15日转化数据_京东渠道_{today}.xlsx', engine='xlsxwriter') as writer:
            df_model_group_now_jd.to_excel(writer, sheet_name='当日机型转化')
            df_model_group_7_jd.to_excel(writer, sheet_name='近7日机型转化')
            df_model_group_15_jd.to_excel(writer, sheet_name='近15日机型转化')
        with pd.ExcelWriter(path + f'芝麻商品/近15日芝麻转化数据_商品ID_{today}.xlsx', engine='xlsxwriter') as writer:
            df_zm_uv.to_excel(writer, sheet_name='当日商品ID转化')
            df_zm_uv_7.to_excel(writer, sheet_name='近7日商品ID转化')
            df_zm_uv_15.to_excel(writer, sheet_name='近15日商品ID转化')
        with pd.ExcelWriter(path + f'搜索商品/近15日搜索转化数据_商品ID_{today}.xlsx', engine='xlsxwriter') as writer:
            df_ss_uv.to_excel(writer, sheet_name='当日商品ID转化')
            df_ss_uv_7.to_excel(writer, sheet_name='近7日商品ID转化')
            df_ss_uv_15.to_excel(writer, sheet_name='近15日商品ID转化')
        print('数据写入完毕！')
        del df_model_group_now, df_model_group_7, df_model_group_15, df_model_group_now_zm, df_model_group_7_zm, df_model_group_15_zm, df_model_group_now_jd, df_model_group_7_jd, df_model_group_15_jd, df_zm_uv, df_zm_uv_7, df_zm_uv_15, df_ss_uv, df_ss_uv_7, df_ss_uv_15
        gc.collect()
        print("回收内存执行完毕！\n")

    def zfb_order(self, hour, minute, path, hours):
        # 判断当前日期是不是本月的第一天，如果是则从上个月第一天开始取数，如果不是则从本月第一天开始取数
        # if datetime.today().strftime('%d')=='01':
        #     date = ((datetime.today()-pd.DateOffset(months=1)).replace(day=1)).strftime('%Y-%m-%d')
        # else:
        #     date = (datetime.today().replace(day=1)).strftime('%Y-%m-%d')
        date = (datetime.now()-pd.DateOffset(months=1)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        print('正在查询数据...')
        df, df_risk, df_risk_examine, df_re, df_ra = self.run(date, hours)
        print('数据查询完毕！\n正在清理数据...')
        df, df2 = self.clean_data(df, df_risk, df_re, df_ra)
        print('数据清理完毕！\n获取支付宝订单数据...')
        df_zfb = df[df.order_type=='ZFB_ORDER']
        df2_zfb = df2[df2.order_type=='ZFB_ORDER']
        df_zfb_group = self.all_model.data_group(df_zfb, df2_zfb, df_risk_examine, '下单日期')
        df_zfb_group = df_zfb_group[["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
                                "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
                                "出库前风控强拒", "待审核", '出库', '拒量出库', '进件出库率',
                                '总体进件出库率（含拒量）', '拒量进件出库率增加', '取消率', '人审拒绝率', '出库前强拒比例', '无法联系占比', '订单出库率']].fillna(0)
        print('支付宝订单数据获取完毕！\n获取提醒信息...')
        df_zfb_group_new = df_zfb_group[:-1].copy()
        # 取到前天和大前天的环比
        qc_num = df_zfb_group_new.去重订单数.pct_change().iloc[-2]
        jj_num = df_zfb_group_new.进件数.pct_change().iloc[-2]
        df_zfb_group_new.loc[:, '出库率'] = pd.to_numeric(df_zfb_group['总体进件出库率（含拒量）'].str.replace('%', ''))/100
        ck_num = df_zfb_group_new.出库率.diff().iloc[-2]
        # df_hb = pd.concat([df_zfb_group_new.去重订单数.pct_change(),df_zfb_group_new.进件数.pct_change(),df_zfb_group_new.出库率.diff().pct_change()])
        # with pd.ExcelWriter('F:/需求/七月需求/支付宝订单环比.xlsx', engine='xlsxwriter') as writer:
        #     df_zfb_group_new.to_excel(writer, sheet_name='明细数据')
        #     df_hb.to_excel(writer, sheet_name='环比数据')
        num_list = [qc_num, jj_num, ck_num]
        name_list = ['去重订单数', '进件数', '出库率']
        tips_list = [0.05, 0.02, 0.01]
        message = ''
        # 计算环比是否跌出预设值，如跌出则设置提醒信息
        for idx, num in enumerate(num_list):
            if num < -tips_list[idx]:
                message += f'''{name_list[idx]}的环比为：{str(round(num*100, 2))+'%'}，跌出：{str(round(tips_list[idx]*100, 2))+'%'}；\n'''
        if message != '':
            messages = '支付宝订单异常提醒\n' + message + '警报！警报！警报！'
        else:
            messages = ''
        print('提醒信息获取完毕！\n正在写入数据...')

        with pd.ExcelWriter(path + f'支付宝订单/近1个月支付宝订单数据_{today}.xlsx', engine='xlsxwriter') as writer:
            df_zfb_group[:-1].to_excel(writer, sheet_name='支付宝订单')
        print('数据写入完毕！')
        self.clean.send_dingtalk_message(self.webhook, self.secret, messages)
        del df_zfb_group, df_zfb_group_new, df_zfb, df2_zfb
        gc.collect()
        print("回收内存执行完毕！\n")

    # 机型转化——月度
    def month_model(self, hour, minute, path, hours, is_current_month=False):
        # 根据is_current_month参数决定获取当月还是上个月的数据
        if is_current_month:
            # 如果是获取当月数据，使用当前月份的第一天
            date = datetime.today().replace(day=1).strftime('%Y-%m-%d')
        else:
            # 如果是获取上个月数据，使用上个月的第一天
            date = ((datetime.today() - pd.DateOffset(months=1)).replace(day=1)).strftime('%Y-%m-%d')

        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        print('正在查询数据...')
        df, df_risk, df_risk_examine, df_re, df_ra = self.run(date, hours)
        print('数据查询完毕！\n正在清理数据...')
        df, df2 = self.clean_data(df, df_risk, df_re, df_ra)
        print('数据清理完毕！\n正在计算机型转化数据...')
        df = df[df.是否二手 == 0]
        df2 = df2[df2.是否二手 == 0]
        def channel(df, df2):
            df_model_group = self.all_model.data_group(df[:-1], df2[:-1], df_risk_examine, '机型内存')
            df_model_group.loc[:, '出库'] = df_model_group.出库 + df_model_group.拒量出库
            df_model_group = df_model_group[
                ["去重订单数", "前置拦截", "拦截率", "进件数", "预授权通过率", "机审强拒", "强拒比例", "机审通过件",
                 "人审拒绝", "风控通过件", "风控通过率", "客户取消", "无法联系",
                 "出库前风控强拒", "待审核", '出库', '总体进件出库率（含拒量）', '取消率', '人审拒绝率', '出库前强拒比例',
                 '无法联系占比', '订单出库率']].fillna(0).sort_values(by='去重订单数', ascending=False)
            return df_model_group
        # 总体
        df_model_group = channel(df, df2)
        # 芝麻租物
        df_zm = df[df.归属渠道=='芝麻租物']
        df2_zm = df2[df2.归属渠道=='芝麻租物']
        df_model_group_zm = channel(df_zm, df2_zm)
        # 京东渠道
        df_jd = df[df.归属渠道 == '京东渠道']
        df2_jd = df2[df2.归属渠道 == '京东渠道']
        df_model_group_jd = channel(df_jd, df2_jd)
        print('机型转化数据计算完毕！\n正在写入数据...')
        with pd.ExcelWriter(path + f'机型转化/月度机型转化_{today}.xlsx', engine='xlsxwriter') as writer:
            df_model_group.to_excel(writer, sheet_name='月度机型转化')
            df_model_group_zm.to_excel(writer, sheet_name='芝麻租物月度机型转化')
            df_model_group_jd.to_excel(writer, sheet_name='京东渠道月度机型转化')
        print('数据写入完毕！')
        del df_model_group, df_model_group_zm, df_model_group_jd
        gc.collect()
        print("回收内存执行完毕！\n")
    # 京东每月签收订单数据
    def jd_qs_order_by_month(self, hour, minute, path):
        date = ((datetime.today() - pd.DateOffset(months=1)).replace(day=1)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y%m%d')
        print(f'执行定时任务：现在是{today}的{hour}点{minute}分...')
        print('正在查询数据...')
        sql_jd_by_month = '''
                select distinct o.order_number, od.new_actual_money 买断价, tojr.create_time from db_digua_business.t_order as o 
                left join db_digua_business.t_order_details as od on o.id = od.order_id 
                left join db_digua_business.t_order_jd_request tojr on tojr.order_number=o.order_number
                where 
                -- date_format(tojr.create_time, '%Y-%m-%d')>='2025-07-01'
                -- and date_format(tojr.create_time, '%Y-%m-%d')<='2025-08-05'
                tojr.create_time>=date_add(current_date, interval -1 month)
                and tojr.request_json like '%IN_THE_LEASE%'
                '''
        df_jd_by_month = self.clean.query(sql_jd_by_month)
        df_jd_by_month = df_jd_by_month.sort_values(by='create_time', ascending=False).groupby('order_number').head(1)
        df_jd_by_month.loc[:, '同步日期'] = df_jd_by_month.create_time.dt.strftime('%Y-%m-%d')
        # df_jd_by_month.loc[:, '合计买断价'] = df_jd_by_month.买断价
        df_jd_by_month.rename(columns={'order_number': '订单号','买断价':'合计买断价'}, inplace=True)

        print('京东每月签收订单数据计算完毕！\n正在写入数据...')
        with pd.ExcelWriter(path + f'京东每月签收订单数据_{today}.xlsx', engine='xlsxwriter') as writer:
            df_jd_by_month.to_excel(writer, sheet_name='京东月签收订单数据', index=False)

        print('数据写入完毕！')
        del df_jd_by_month
        gc.collect()
        print("回收内存执行完毕！\n")

if __name__ == '__main__':
    cd = Conversion_Data()
    hour = 18
    minute = 10
    path = r'\\digua\迪瓜租机\002数据监测\3.转化数据/'
    path1 = r'\\digua\迪瓜租机\002数据监测\9.京东每月签收订单明细/'
    # cd.zfb_order(9, minute, path, 24)
    # cd.jd_job(10, 15, path, 24)
    # cd.my_job(hour, minute, path, 18)
    print('正在创建定时任务...')
    scheduler = BackgroundScheduler()
    # 每天18点10分开始执行，获取每天18点前的数据
    job = scheduler.add_job(cd.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path, 18])
    # 每天10点15分开始执行
    job_jd = scheduler.add_job(cd.jd_job, 'cron', hour=10, minute=15, args=[10, 15, path, 24])
    # 每天9点10分开始执行
    job_zfb = scheduler.add_job(cd.zfb_order, 'cron', hour=9, minute=10, args=[9, 10, path, 24])
    # 每月1号11点1分开始执行, 最后一个参数False表示获取上个月数据
    job_month = scheduler.add_job(cd.month_model, CronTrigger(day=1, hour=11, minute=1), args=[11, 1, path, 24, False])
    # 新增任务：每月16号上午10点1分执行获取当月的机型转化数据
    job_month_current = scheduler.add_job(
        cd.month_model,
        CronTrigger(day=16, hour=10, minute=1),
        args=[10, 1, path, 24, True]
    )

    # 每月1号10点1分开始执行
    job_jd_month = scheduler.add_job(cd.jd_qs_order_by_month, CronTrigger(day=1, hour=10, minute=1), args=[10, 1, path1])

    # cd.month_model(11, 1, path, 24, False)

    # cd.jd_qs_order_by_month(10, 1, path1)
    print('定时任务创建完毕...\n正在执行定时任务...')
    # 查看是否添加了任务
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job.next_run_time
            next_run_time_jd = job_jd.next_run_time
            next_run_time_zfb = job_zfb.next_run_time
            next_run_time_month = job_month.next_run_time
            next_run_time_jd_month = job_jd_month.next_run_time
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
            elif next_run_time_zfb:
                now = datetime.now(timezone.utc)
                sleep_duration_zfb = (next_run_time_zfb - now).total_seconds()
                if sleep_duration_zfb > 0:
                    time.sleep(sleep_duration_zfb)
            elif next_run_time_month:
                now = datetime.now(timezone.utc)
                sleep_duration_month = (next_run_time_month - now).total_seconds()
                if sleep_duration_month > 0:
                    time.sleep(sleep_duration_month)
            elif next_run_time_jd_month:
                now = datetime.now(timezone.utc)
                sleep_duration_jd_month = (next_run_time_jd_month - now).total_seconds()
                if sleep_duration_jd_month > 0:
                    time.sleep(sleep_duration_jd_month)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
        gc.collect()