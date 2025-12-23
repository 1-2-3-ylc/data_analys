import numpy as np
import pandas as pd
import xlwings as xw
import matplotlib.pyplot as plt
plt.rcParams["font.sans-serif"]=["SimHei"]
plt.rcParams["axes.unicode_minus"]=False
from apscheduler.schedulers.background import BackgroundScheduler
import calendar
import warnings
import gc
warnings.filterwarnings("ignore")
import pymysql
import time
from datetime import timedelta , datetime, timezone
from Class_Model.All_Class import All_Model, Week_Model, Data_Clean

class Channel_Analyse:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        # pd.set_option('display.max_rows', None)
        # 实例化All_Model类
        self.all_models = All_Model()
        self.week_models = Week_Model()
        self.clean = Data_Clean()
        # 2025-06-20 设置归属渠道top15, '邦道-小程序-手机'
        self.top15_list = ['京东渠道', '芝麻租物', '搜索渠道', '单人聊天会话中的小程序消息卡片（分享）', '八派信息', '支付宝客户端首页', '其他渠道场景渠道。', '未知渠道', '派金花', '我的小程序入口', '抖音渠道', '支付宝直播', '小程序商家消息（服务提醒）', '订阅消息']
        # 替换为你的 Webhook 地址
        self.webhook = "https://oapi.dingtalk.com/robot/send?access_token=d4072f19c1ebe08ea7a71a22df26337eb2fb51327c0ffeac14f8b53b4ed29c78"
        # 替换为你的密钥，如果没有设置则留空
        self.secret = "SEC953fc60a7f3cec15501e044bbe0f93d3bcbb5d68cb6628599f6a0eff94a2a6d4"


    # 获取所需数据
    def get_data(self, hour):
        sql1 = f''' -- 订单&风控信息  近10日数据   
        SELECT om.create_time,om.id as order_id ,om.order_number
        ,om.status
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
        , om.order_type, tojo.app_type, tor.update_time, tomt.reason, tp.classify_id
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
        -- 京东外部订单关联表
        left join db_digua_business.t_order_jd_out_no tojo on tojo.order_id=om.id
        -- 商家订单转移表
        left join db_digua_business.t_order_merchant_transfer tomt on tomt.order_id=om.id
        -- 商品表
        left join db_digua_business.t_product tp on tp.id=tod.product_id
        where om.user_mobile is not null 
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        and  om.create_time >= DATE_ADD(CURRENT_DATE,INTERVAL -1 month )
        and hour(om.create_time)<{hour}
        -- and DATE_FORMAT(om.create_time, '%H%i')<= '1630'
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

        return df_order, df_risk, df_risk_examine, df_re, df_ra

    # 数据清理
    def clean_data(self, df, df_risk, df_re, df_ra):
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
        # print(df.来源渠道=='邦道-小程序-手机')
        # 渠道归属
        df.loc[:, "归属渠道"] = df.apply(
            lambda x: self.clean.qudao_type(x["来源渠道"], x["activity_name"], x["order_method"], x['channel_type_id'],x['order_type']), axis=1)
        # print(df.归属渠道.unique())
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
                                        x["无法联系原因"], x["total_describes"], x['是否前置拦截'], x['是否机审强拒'],
                                        x['是否出库前风控强拒']), axis=1)
        # 获取节点状态数据
        df = self.clean.status_node(df)
        # 保留商家数据
        df_contain = df.copy()
        # 剔除商家数据
        df = self.clean.drop_merchant(df)

        # 剔除据量数据
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        df2 = df2[df2.是否拒量 == 0]

        return df, df2, df_contain

    # 获取近15天进件top15的归属渠道
    def get_top15(self, df, df2, df_risk_examine):
        today = datetime.now() - timedelta(days=7)
        df = df[df.下单日期 >= today]
        df2 = df2[df2.下单日期 >= today]
        df_top15 = self.all_models.data_group(df, df2, df_risk_examine, '归属渠道')[['进件数']]
        df_top15 = df_top15.sort_values(by='进件数', ascending=False)[:15].index.to_list()
        return df_top15

    # 获取对比数据结果
    def get_result(self, df, value, count):
        now_before = datetime.now() - pd.DateOffset(months=1)
        year, month = now_before.year, now_before.month
        # 获取上个月月天数
        days_in_month = calendar.monthrange(year, month)[1]
        # 获取对比数据
        df = df[(df.归属渠道.isin(self.top15_list))]  #
        df_pivot = pd.pivot_table(df, values=value, columns='归属渠道', index='下单日期', aggfunc=count)
        df_pivot.index = df_pivot.index.astype('str')
        df_pivot_new = df_pivot.copy()
        df_pivot_new.loc['昨日对比', :] = df_pivot.diff(periods=1).iloc[-1]
        df_pivot_new.loc['周环比', :] = df_pivot.diff(periods=7).iloc[-1]
        df_pivot_new.loc['月同比', :] = df_pivot.diff(periods=30 + (days_in_month - 30)).iloc[-1]

        return df_pivot_new.fillna(0)[self.top15_list].fillna(0)
    # def get_result(self, df, value, func):
    #     try:
    #         # 创建透视表
    #         df_pivot = pd.pivot_table(df, values=value, index='下单日期', columns='渠道名称', aggfunc=func)
    #
    #         # 确保所有top15_list中的列都存在，如果不存在则添加值为0的列
    #         missing_cols = [col for col in self.top15_list if col not in df_pivot.columns]
    #         for col in missing_cols:
    #             df_pivot[col] = 0
    #
    #         # 只保留top15_list中的列，并填充缺失值为0
    #         df_pivot_new = df_pivot[self.top15_list].fillna(0)
    #
    #         # 确保列的顺序与top15_list一致
    #         df_pivot_new = df_pivot_new[self.top15_list]
    #
    #         return df_pivot_new
    #
    #     except Exception as e:
    #         print(f"处理数据时出错: {str(e)}")
    #         # 返回一个空的DataFrame，列名为top15_list
    #         return pd.DataFrame(columns=self.top15_list)

    # 审核时长提醒
    def audit(self, path):
        print('正在获取审核订单数据...')
        sql_audit = '''
        select 
        om.create_time 下单时间, om.order_number 订单号, om.merchant_name 商家, om.day 租赁天数
        from db_digua_business.t_order om
        where om.`status` not in (1, 13) and om.`status`=12 and om.merchant_name!='线下小店'
        '''
        df_audit = self.clean.query(sql_audit)
        print('审核订单数据获取完毕！\n正在获取审核数据...')
        now_date = datetime.now()
        # 获取审核时间超过24小时不超过48小时和审核时间超过48小时
        df_audit_1 = df_audit[(now_date - df_audit.下单时间 >= pd.Timedelta(hours=24))&(now_date - df_audit.下单时间 <= pd.Timedelta(hours=48))]
        df_audit_2 = df_audit[now_date - df_audit.下单时间 >= pd.Timedelta(hours=48)]
        print('审核数据获取完毕！\n正在获取审核提醒...')
        def audit_message(df, day):
            # 获取租赁天数的订单数
            df_g = df.groupby('租赁天数').agg({'订单号': 'count'})
            audit_list = df_g.index.to_list()
            message_audit = ''
            # 构建提醒信息（钉钉运营数据监测群）
            for al in audit_list:
                message_audit += f'''审核时长超过{day}天的审核订单中，租赁天数为{al}天的订单待审核数为：{df_g.loc[al, '订单号']}！\n'''
            return message_audit
        message_1 = audit_message(df_audit_1, 1)
        message_2 = audit_message(df_audit_2, 2)
        message = message_1 + message_2
        print('审核提醒获取完毕！\n正在写入数据...')
        today = datetime.now().strftime('%Y-%m-%d')
        with pd.ExcelWriter(path+f'审核提醒_{today}.xlsx', engine='xlsxwriter') as writer:
            df_audit_1.to_excel(writer, sheet_name='审核时长超过24小时', index=False)
            df_audit_2.to_excel(writer, sheet_name='审核时长超过48小时', index=False)
        print('数据写入完毕！\n')
        # 钉钉机器人发送数据
        self.clean.send_dingtalk_message(self.webhook, self.secret, message)
        del df_audit, df_audit_1, df_audit_2
        gc.collect()
        print("回收内存执行完毕！\n")

    def run(self, hour):
        # 获取数据
        print('正在获取数据...')
        df, df_risk, df_risk_examine, df_re, df_ra = self.get_data(hour)
        # 数据处理
        print('数据获取完毕！\n正在清理数据...')
        df, df2, df_contain = self.clean_data(df, df_risk, df_re, df_ra)

        return df, df2, df_risk_examine, df_contain

    # 电脑品类
    def classify_pc(self, path, hour):
        df, df2, df_risk_examine, df_contain = self.run(hour)
        Today = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today}')
        # 电脑品类
        classify_list = [125, 126, 127, 185]
        def classify_channel(key, key_classify):
            # 获取自营和商家的分组汇总数据
            if key_classify =='自营':
                df_ = df[(df.classify_id.isin(classify_list)) & (df.order_type == key)]
                df2_ = df2[(df2.classify_id.isin(classify_list)) & (df.order_type == key)]
                df_group = self.all_models.data_group(df_, df2_, df_risk_examine, '下单日期')
                return df_group[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系",
                    "出库前风控强拒","待审核",'出库', '进件出库率','取消率','人审拒绝率','出库前强拒比例','无法联系占比','订单出库率']]
            elif key_classify=='商家':
                df_contain_ = df_contain[(df_contain.classify_id.isin(classify_list))&(df.order_type == key)]
                df_group = self.all_models.merchant_names(df_contain_, '汇客好租', '下单日期')
                return df_group[["去重订单数","前置拦截","拦截率","进件数","预授权通过率","机审强拒","强拒比例","机审通过件","人审拒绝","风控通过件","风控通过率","客户取消","无法联系",
                    "出库前风控强拒","待审核",'出库', '进件出库率','取消率','人审拒绝率','订单出库率']]
        # 自营京东
        df_jd_group = classify_channel('JD_ORDER', '自营')
        # 自营支付宝
        df_zfb_group = classify_channel('ZFB_ORDER', '自营')
        # 汇客好租——京东
        hkhz_jd_group = classify_channel('JD_ORDER', '商家')
        # 汇客好租——支付宝
        hkhz_zfb_group = classify_channel('ZFB_ORDER', '商家')
        today = datetime.now().strftime('%Y%m%d')
        with pd.ExcelWriter(path+f'电脑品类渠道分析_{today}.xlsx', engine='xlsxwriter') as writer:
            df_jd_group.to_excel(writer, sheet_name='京东自营')
            df_zfb_group.to_excel(writer, sheet_name='支付宝自营')
            hkhz_jd_group.to_excel(writer, sheet_name='京东商家')
            hkhz_zfb_group.to_excel(writer, sheet_name='支付宝商家')

        del df_jd_group, df_zfb_group, hkhz_jd_group, hkhz_zfb_group
        gc.collect()
        print("回收内存执行完毕！\n")

    # 设置定时任务
    def my_job_channel(self, path, hour, path1=None):
        df, df2, df_risk_examine, df_contain = self.run(hour)
        # 获取进件top15的归属渠道
        # df_top15 = self.get_top15(df, df2, df_risk_examine)
        # print(df_top15)
        # df = df[(df.归属渠道.isin(df_top15))]
        Today = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today}的{hour}')
        # 获取对比数据
        print('数据清理完毕！\n正在获取对比数据...')
        df_pivot_new_qc = self.get_result(df, 'order_id', 'count')
        df_pivot_new_jj = self.get_result(df, '是否进件', 'sum')
        print('对比数据获取完毕！\n正在写入数据...')
        today = datetime.now().strftime('%Y%m%d%H%M%S')
        with pd.ExcelWriter(path+f'渠道对比分析_{today}.xlsx', engine='xlsxwriter') as writer:
            df_pivot_new_qc.to_excel(writer, sheet_name='去重订单')
            df_pivot_new_jj.to_excel(writer, sheet_name='进件')
        print('数据写入完毕！')
        # 审核提醒
        if path1 is not None:
            self.audit(path1)
        del df_pivot_new_qc, df_pivot_new_jj
        gc.collect()
        print("回收内存执行完毕！\n")

if __name__ == '__main__':
    path = r'\\digua\迪瓜租机\002数据监测\1.渠道对比/'
    path1 = r'\\digua\迪瓜租机\002数据监测\4.审核提醒/'
    path2 = r'\\digua\迪瓜租机\002数据监测\8.电脑品类渠道/'
    ca = Channel_Analyse()
    # ca.audit(path1)
    # ca.classify_pc(path2, 24)
    # 实时需要，hour为sql中时间区间
    ca.my_job_channel(path, 18)
    # ca.my_job_channel(path, 14, path1)

    scheduler = BackgroundScheduler()
    # 定时调度，前四个参数分别为调度器任务函数、触发器类型、调度时间，最后一个参数为传给任务函数的参数（sql时间范围）
    # 每天14点开始执行
    job_channel = scheduler.add_job(ca.my_job_channel, 'cron', hour=14, minute=1, args=[path, 14, path1])
    # 每天18点开始执行
    job1_channel = scheduler.add_job(ca.my_job_channel, 'cron', hour=18, minute=1, args=[path, 18])
    # 每天9点开始执行
    classify_channel = scheduler.add_job(ca.classify_pc, 'cron', hour=9, minute=1, args=[path2, 24])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    # 查看是否添加了任务
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job_channel.next_run_time
            next_run_time1 = job1_channel.next_run_time
            next_run_time_classify = classify_channel.next_run_time
            if next_run_time:
                now = datetime.now(timezone.utc)
                sleep_duration = (next_run_time - now).total_seconds()
                if sleep_duration > 0:
                    time.sleep(sleep_duration)
            elif next_run_time1:
                now = datetime.now(timezone.utc)
                sleep_duration1 = (next_run_time1 - now).total_seconds()
                if sleep_duration1 > 0:
                    time.sleep(sleep_duration1)
            elif next_run_time_classify:
                now = datetime.now(timezone.utc)
                sleep_duration_classify = (next_run_time_classify - now).total_seconds()
                if sleep_duration_classify > 0:
                    time.sleep(sleep_duration_classify)
            else:
                time.sleep(60)  # 如果没有找到下次运行时间，则等待一段时间后重新检查
    except (KeyboardInterrupt, SystemExit):
        # 用户按下 Ctrl+C 或系统要求退出时，优雅地关闭调度器
        scheduler.shutdown()
        gc.collect()