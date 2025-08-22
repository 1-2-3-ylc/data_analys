import pandas as pd
import numpy as np
import pymysql
from datetime import datetime, timezone
import time
import warnings
warnings.filterwarnings('ignore')
from apscheduler.schedulers.background import BackgroundScheduler
from Class_Model.All_Class import Data_Clean


class Scheduled:
    def __init__(self):
        self.clean = Data_Clean()
    # 从数据库中提取近16天内，排除特定用户和订单状态的数据，并解析订单风险决策结果中的JSON字段
    def get_sql(self):
        sql = '''
        SELECT 
        om.create_time, om.id as order_id, tor.decision_result, top.total_describes, om.status, tmu.id_card_num, pa.name as activity_name, tmu.true_name
        ,om.user_mobile, om.merchant_name
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.tips') end,'"','') as tips   
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.is_vip') end,'"','') as is_vip
        ,replace(case when JSON_VALID(tor.decision_result) THEN JSON_EXTRACT(tor.decision_result, '$.status') end,'"','') as status_result 
        from db_digua_business.t_order  om
        inner join  (select  t.order_id  from db_digua_business.t_user_verify_order_record t  GROUP BY 1) tuv  on om.id = tuv.order_id 
        left join db_digua_business.t_order_risk tor on om.id = tor.order_id 
        left join ( SELECT  t.order_id,JSON_ARRAYAGG(t.describes) as total_describes
        from db_digua_business.t_order_personnel t   GROUP BY 1 ) top  on om.id = top.order_id 
        left join db_digua_business.t_member_user tmu on om.user_id = tmu.id
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        where  om.create_time > DATE_ADD(CURRENT_DATE,INTERVAL -16 day )
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        and om.user_mobile is not null
        '''
        df = self.clean.query(sql)
        return df

    def clean_date(self, df):
        df.loc[:, '下单日期'] = df.create_time.dt.date
        df.drop(df[df['merchant_name'].str.contains(pat='探路者', regex=False) == True].index, inplace=True)
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

        # 删除重复订单
        df.drop_duplicates(subset=["order_id"], inplace=True)
        df.drop_duplicates(subset=["true_name", "user_mobile", "id_card_num", "下单日期"], keep="last", inplace=True)

        df.drop(df[df['true_name'].isin(
            ["刘鹏", "谢仕程", "潘立", "洪柳", "陈锦奇", "周杰", "卢腾标", "孔靖", "黄娟", "钟福荣", "邱锐杰", "唐林华"
                , "邓媛斤", "黄子南", "刘莎莎", "赖瑞彤", "孙子文"])].index, inplace=True)
        return df

    def get_date(self, df):
        # 排除拒量
        df.drop(df[df['merchant_name'] == "小蚂蚁租机"].index, inplace=True)
        df.drop(df[df['merchant_name'] == "兴鑫兴通讯"].index, inplace=True)
        df = df[~(df.tips.str.contains(r'策略2412|命中自有模型回捞策略|回捞策略250330命中') == True)]

        df.loc[:, '免人审'] = np.where((df.is_vip=='1')&(df.status_result=='0'), 1, 0)
        df.loc[:, '审核拒绝'] = np.where(df.total_describes.str.contains('审核不通过：')==True, 1, 0)
        df.loc[:, '免审拒绝'] = np.where((df.total_describes.str.contains('审核不通过：')==True)&(df.免人审==1), 1, 0)
        df.loc[:, '客户取消'] = np.where((df.total_describes.str.contains('客户申请取消')==True), 1, 0)
        df.loc[:, '无法联系'] = np.where((df.total_describes.str.contains('用户无法联系')==True), 1, 0)
        df.loc[:, '命中出库前风控流强拒'] = np.where((df.decision_result.str.contains('命中出库前风控流强拒')==True), 1, 0)
        df.loc[:, '命中出库前风控流强拒蚂蚁数控'] = np.where((df.total_describes.str.contains('蚂蚁数控风险等级')==True), 1, 0)
        df.loc[:, '免审客户取消'] = np.where((df.total_describes.str.contains('客户申请取消')==True)&(df.免人审==1), 1, 0)
        df.loc[:, '免审无法联系'] = np.where((df.total_describes.str.contains('用户无法联系')==True)&(df.免人审==1), 1, 0)
        df.loc[:, '免审命中出库前风控流强拒'] = np.where((df.decision_result.str.contains('命中出库前风控流强拒')==True)&(df.免人审==1), 1, 0)
        df.loc[:, '免审命中出库前风控流强拒蚂蚁数控'] = np.where((df.total_describes.str.contains('蚂蚁数控风险等级')==True)&(df.免人审==1), 1, 0)
        df.loc[:, '出库'] = np.where((df.status.isin([2,3,4,5,6,8,15])), 1, 0)
        df.loc[:, '免审出库'] = np.where((df.status.isin([2,3,4,5,6,8,15]))&(df.免人审==1), 1, 0)
        # df.loc[:, '非免审出库'] = np.where((df.出库==1)&(df.免审出库==0), 1, 0)

        df_group = df.groupby('下单日期').agg({'order_id': 'count', '免人审': 'sum', '审核拒绝': 'sum', '免审拒绝': 'sum', '客户取消': 'sum', '无法联系': 'sum'
                                            , '命中出库前风控流强拒': 'sum', '命中出库前风控流强拒蚂蚁数控': 'sum', '免审客户取消': 'sum',
                                            '免审无法联系': 'sum', '免审命中出库前风控流强拒': 'sum', '免审命中出库前风控流强拒蚂蚁数控': 'sum',
                                            '出库': 'sum', '免审出库': 'sum'}).rename(columns={'order_id': '进入信审', '免人审': '免审订单'})
        df_group.loc[:, '非免审进件'] = df_group.进入信审-df_group.免审订单
        df_group.loc[:, '非免审出库'] = df_group.出库-df_group.免审出库
        df_group.loc[:, '免审出库比例'] = (df_group.免审出库/df_group.免审订单).map(lambda x: format(x, '.2%'))
        df_group.loc[:, '非免审出库比例'] = (df_group.非免审出库/df_group.非免审进件).map(lambda x: format(x, '.2%'))
        df_group = df_group[['进入信审','免审订单', '非免审进件', '审核拒绝', '免审拒绝', '客户取消', '无法联系', '命中出库前风控流强拒', '命中出库前风控流强拒蚂蚁数控'
                            , '免审客户取消','免审无法联系', '免审命中出库前风控流强拒', '免审命中出库前风控流强拒蚂蚁数控','出库','免审出库', '非免审出库'
                            , '免审出库比例', '非免审出库比例']]
        return df_group


    def run(self):
        print('正在查询数据...')
        df = self.get_sql()
        print('数据查询完毕...\n正在清理数据...')
        df = self.clean_date(df)
        print('数据清理完毕...\n正在获取数据...')
        df_group = self.get_date(df)
        print('数据获取完毕...')
        return df_group


    # 创建定时任务
    def my_job(self, hour, minute, path):
        Today2 = str(datetime.now().strftime('%Y%m%d%H'))
        print(f'执行定时任务：现在是{Today2}的{hour}:{minute}')
        df_group = self.run()
        with pd.ExcelWriter(path + f'每日信审需求数据2_{Today2}.xlsx', engine='xlsxwriter') as writer:
            df_group.to_excel(writer, sheet_name='每日进入信审数据')

if __name__ == '__main__':
    hour = 18
    minute = 21
    path = r'\\digua\迪瓜租机\13.运营-信审每日数据/'
    scheduled = Scheduled()
    scheduler = BackgroundScheduler()
    # 每天的18点21执行一次
    job = scheduler.add_job(scheduled.my_job, 'cron', hour=hour, minute=minute, args=[hour, minute, path])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    print(scheduler.get_jobs())
    scheduler.start()


    # scheduled.my_job(hour, minute, path)
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
