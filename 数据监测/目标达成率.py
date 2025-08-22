import pandas as pd
import numpy as np
import pymysql
import time
import warnings
warnings.filterwarnings('ignore')
from datetime import datetime, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from Class_Model.All_Class import Data_Clean

class Small_Page:
    def __init__(self):
        self.clean = Data_Clean()


    def select_data(self, year, date):
        sql_df = f'''
        SELECT om.create_time,om.id as order_id, om.status
        ,case om.`status` when  1 then "待支付" when  2 then "待发货" when  3 then "待收货" when  4 then "租赁中" when  5 then "待归还" 
        when  6 then "待商家收货" when  7 then "退押中" when  8 then "已完成" when  10 then "已退款" when  11 then "待退押金" when  12 then "待审核" 
        when  13 then "订单取消" when  15 then "检测中" when  9999 then "逾期订单" end as status2 
        ,om.user_mobile,tmu.true_name,tmu.id_card_num, pa.type
        ,pa.name as activity_name,top.total_describes, tod.sku_attributes, om.merchant_name
        from  db_digua_business.t_order  om
        -- 备注信息合并 
        left join ( SELECT  t.order_id,JSON_ARRAYAGG(t.describes) as total_describes from db_digua_business.t_order_personnel t   GROUP BY 1 ) top on om.id = top.order_id 
        -- 活动名称
        left join db_digua_business.t_platform_activity pa on om.activity_id = pa.id
        -- 用户信息 
        left join db_digua_business.t_member_user tmu on om.user_id = tmu.id
        -- 订单详情表
        left join db_digua_business.t_order_details tod on tod.order_id=om.id
        where om.user_mobile is not null 
        and tmu.true_name not in ("刘鹏","谢仕程","潘立","洪柳","陈锦奇","周杰","卢腾标","孔靖","黄娟","钟福荣","邱锐杰","唐林华"
        ,"邓媛斤","黄子南","刘莎莎","赖瑞彤","孙子文",'张娜','罗文龙','孔靖','彭康力','何薪华','夏玥','潘佳','包闻天','方全龙','李楠','向圆圆','黄兰娟','林婉婷','廖丽敏','李巧玲','李巧凤','刘三妹','蔡斯静','陈宜诗','陈宝易','林寅钗','谢金凤','刘宏生','骆昌鑫','何静', '李珍珍')
        and date_format(om.create_time, '%Y-%m-%d')>='{year}-01-01'
        and date_format(om.create_time, '%Y-%m-%d')<'{date}'
        '''
        df = self.clean.query(sql_df)
        return df

    def clean_data(self, df):
        # 排除号卡活动的订单
        df = df[df.type!=4]
        #处理日期
        df["下单日期"] = df["create_time"].dt.date
        df["下单日期"] = pd.to_datetime(df["下单日期"], errors="coerce")
        df["下单月份"] = df["create_time"].dt.strftime('%Y-%m')
        df['年份'] = df["下单日期"].dt.year
        df['月份'] = df["下单日期"].dt.month
        # 处理备注信息
        df["取消原因"] = df["total_describes"].str.split("客户申请取消：").str[1].str.split("$").str[0].str.strip()
        df["电审拒绝原因"] = df["total_describes"].str.split("审核不通过：").str[1].str.split("$").str[0].str.strip()
        df["无法联系原因"] = df["total_describes"].str.split("用户无法联系：").str[1].str.split("$").str[0].str.strip()
        # 订单去重
        df = self.clean.order_drop_duplicates(df)
        # 删除商家和拒量数据
        df = self.clean.drop_merchant(df)
        df2 = df.copy()
        df2 = self.clean.drop_rejected_merchant(df2)
        return df2

    def math_data(self, df, year):
        #设置每个月的出库目标数
        target_dict = {
            '01': 2400,
            '02': 1680,
            '03': 4500,
            '04': 5400,
            '05': 6000,
            '06': 6600,
            '07': 6900,
            '08': 7500,
            '09': 8400,
            '10': 9000,
            '11': 10200,
            '12': 11420,
            f'{year}': 80000
        }
        # 计算节点状态
        df.loc[:, '进件'] = np.where(df.status.isin([1, 13]), 0, 1)
        df.loc[:, '人审拒绝'] = np.where((df.进件 == 1) & (~df.电审拒绝原因.isna()) & (df.status2 == '已退款'), 1, 0)
        df.loc[:, '客户取消'] = np.where((df.进件 == 1) & (~df.取消原因.isna()), 1, 0)
        df.loc[:, '无法联系'] = np.where((df.进件 == 1) & (~df.无法联系原因.isna()), 1, 0)
        df.loc[:, '待审核'] = np.where((df.进件 == 1) & (df.status2 == '待审核'), 1, 0)
        df.loc[:, '出库'] = np.where(
            (df['人审拒绝'] == 0) & (df['客户取消'] == 0) & (df['无法联系'] == 0) & (df['待审核'] == 0) & (
                df.status.isin([2, 3, 4, 5, 6, 8, 15])), 1, 0)

        # 计算每个月的出库数和达成率
        df_month = df.groupby('下单月份').agg({'出库': 'sum'})
        df_month.loc[:, '月份'] = df_month.index.str[-2:]
        df_month.loc[:, '月度目标'] = df_month.月份.map(target_dict)
        df_month.loc[:, '月度达成率'] = (df_month.出库/df_month.月度目标).map(lambda x: format(x, '.2%'))

        # 计算截止到本月末的年度出库和达成率
        df_year = df.groupby('年份').agg({'出库': 'sum'})
        df_year = df_year.reset_index()
        df_year.loc[:, '年度目标'] = df_year.年份.astype(str).map(target_dict)
        df_year.loc[:, '年度达成率'] = (df_year.出库/df_year.年度目标).map(lambda x: format(x, '.2%'))

        return df_month, df_year

    def my_job(self, hour, minute, path):
        year = datetime.now().year
        date = datetime.now().strftime('%Y-%m-%d')
        print(f'执行定时任务：现在是{date}的{hour}点{minute}分...')
        print('正在查询数据...')
        df = self.select_data(year, date)
        print('数据查询完毕！\n正在清理数据...')
        df = self.clean_data(df)
        print('数据清理完毕！\n正在计算达成率...')
        df_month, df_year = self.math_data(df, year)
        print('达成率计算完毕！\n正在写入数据...')
        today = datetime.now().strftime('%Y%m%d')
        with pd.ExcelWriter(path + f'达成率_{today}.xlsx', engine='xlsxwriter') as writer:
            df_month.to_excel(writer, sheet_name='月度达成率')
            df_year.to_excel(writer, sheet_name='年度达成率', index=False)
        print('写入数据完毕！')

if __name__ == '__main__':
    hour = 12
    minute = 1
    path = r'\\digua\迪瓜租机\002数据监测\6.达成率/'
    sp = Small_Page()
    # sp.my_job(hour, minute, path)
    scheduler = BackgroundScheduler()
    # 每周五的12点开始执行
    job_channel = scheduler.add_job(sp.my_job, CronTrigger(day_of_week='fri', hour=hour, minute=minute), args=[hour, minute, path])
    print('定时任务创建完毕...\n正在执行定时任务my_job...')
    # 查看是否添加了任务
    print(scheduler.get_jobs())
    scheduler.start()
    # 模拟主程序
    try:
        while True:
            next_run_time = job_channel.next_run_time
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