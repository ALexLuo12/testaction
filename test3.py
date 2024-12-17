from api.repositories.BaseRepository import Repository
from collections import defaultdict




class PdmrRepository(Repository):
    def get_pdmr_info(self, uid):
        params = dict()
        params['uid'] = uid

        statement = '''
                    select ka_name,tab_name from wz_pdmr_mapping
                                        where ka_manager_uid = :uid and type = 1 order by `index` 
                    '''

        return self.list(statement, params)

    def get_nat_info(self,uid):


        params = dict()
        params['uid'] = uid

        statement = '''select ka_name,tab_name from wz_pdmr_mapping
                       where ka_manager_uid = :uid and type = 2 order by `index`'''

        return self.list(statement, params)


    def get_rep_info(self, uid):
        params = dict()
        params['uid'] = uid

        statement = '''select ka_name ,tab_name from wz_pdmr_mapping
                    where ka_manager_uid = :uid and type = 3 order by `index`'''

        return self.list(statement, params)

    def get_P_info(self):
        params = dict()

        statement = '''select  case when length(p_value-1)=1 and p_value > 1 then concat(`P_year`,'P','0',`P_Value`-1) 
     when length(p_value-1)=2 and p_value > 1 then concat(`P_year`,'P',`P_Value`-1) 
     when p_value = 1 then concat(`P_year`-1,'P','13')
end as current_P  from ai_fcst.rc_calendar 
                    where  `date` = date_format(now(),'%%Y-%%m-%%d');
                    '''

        return self.list(statement, params)
        #TODO
        # return [{'current_p':'2024P09'}]

    def get_factP_info(self):
        params = dict()

        statement = '''
        select  
        case when  length(`P_Value`)=1  then concat(`P_year`,'P','0',`P_Value`) 
        else 
        concat(`P_year`,'P',`P_Value`) 
        end  as current_P 
        from ai_fcst.rc_calendar 
                            where  `date` = date_format(now(),'%%Y-%%m-%%d');
                        '''

        return self.list(statement, params)


        # return [{'current_p': '2024P06'}]
    def get_GSV_Forecast_table_name(self,pillar):
        params = dict()
        # params['pillar']=pillar
        statement = '''select file_name from pdmr_file_ctr 
                        where meta_name = 'GSV Forecast table'
                        order by insert_dt  desc
                        limit 1 
                    '''

        return self.list(statement, params)

    # def pillar_lz_load(self, pillar, current_P):
    #     first_sql = """
    #     delete from iz_pdmr_adjusted_target
    #     where Pillar = :Pillar and statics_period = :statics_period;
    #     """

    def pillar_lz_to_iz(self,pillar,current_P):
        params = dict()
        params['Pillar']=pillar
        params['statics_period']=current_P

        first_sql = """
        delete from iz_pdmr_adjusted_target 
        where Pillar = :Pillar and statics_period = :statics_period;
        """
        second_sql = '''
insert into iz_pdmr_adjusted_target
            (select Pillar,
            customer,
            category_code,
            bag_size,
            animal_species,
            gsv,
            product_type,
            volume,
            `year`,
            period,
            Pvalue_Pyear,
            insert_time,
            statics_period
            from 
            (
            select
            a.Pillar,
            a.customer,
            a.category_code,
            a.bag_size,
            a.animal_species,
            a.gsv,
            a.product_type,
            a.`year`,
            a.period,
            a.Pvalue_Pyear,
            a.insert_time ,
            a.statics_period,
            case when b.price = 0 then 0 else
            a.gsv/b.price end as volume 
            from lz_pdmr_adjusted_target a 
            left join iz_pdmr_product_price b 
            on a.customer = b.customer and a.category_code = b.category_code  and a.`year` = b.`Year` 
            and a.period = b.period
            where   a.product_type in ('sellable','fg')  
            and a.Pillar = :Pillar
            and a.statics_period = :statics_period
            and a.Pvalue_Pyear>a.statics_period
            and a.customer <>'VET_EVET'
            union all
            select 
            a.Pillar,
            a.customer,
            a.category_code,
            a.bag_size,
            a.animal_species,
            a.gsv,
            a.product_type,
            a.`year`,
            a.period,
            a.Pvalue_Pyear,
            a.insert_time ,
            a.statics_period,
            a.volume
            from lz_pdmr_adjusted_target a 
            where   a.product_type = 'ancp'
            and a.Pillar = :Pillar
            and a.statics_period = :statics_period
            and a.Pvalue_Pyear>a.statics_period
            and a.customer <>'VET_EVET'
            )a
            )
		        
        '''

        self.insert_or_update(first_sql,params)
        self.insert_or_update(second_sql,params)

        if pillar == 'VET':
            sql = '''
                insert into iz_pdmr_adjusted_target 
	
		      select Pillar,
            customer,
            category_code,
            bag_size,
            animal_species,
            gsv,
            product_type,
            volume,
            `year`,
            period,
            Pvalue_Pyear,
            insert_time,
            statics_period from (
            select 
            a.Pillar,
            a.customer,
            a.category_code,
            a.bag_size,
            a.animal_species,
            a.gsv*pcp.percent as gsv,
            a.product_type,
            a.`year`,
            a.period,
            a.Pvalue_Pyear,
            a.insert_time ,
            a.statics_period,
            case when b.price = 0 then 0 else
            a.gsv/b.price*pcp.percent end as volume
            from lz_pdmr_adjusted_target a 
            left join iz_pdmr_product_price b 
		        on a.customer = b.customer and a.category_code = b.category_code  and a.`year` = b.`Year` 
            and a.period = b.period
            left join pdmr_customer_percent pcp 
            on a.customer = pcp.customer and  concat(a.period,'Y', SUBSTRING(a.`year`,3,2))  = pcp.period and a.statics_period = pcp.static_period 
		     where  a.customer = 'VET_EVET' and a.statics_period = :statics_period and a.Pvalue_Pyear>a.statics_period
		    union all     
            select 
            a.Pillar,
            'VET_EVET 100%%' ascustomer,
            a.category_code,
            a.bag_size,
            a.animal_species,
            a.gsv*(1-pcp.percent) as gsv,
            a.product_type,
            a.`year`,
            a.period,
            a.Pvalue_Pyear,
            a.insert_time ,
            a.statics_period,
            case when b.price = 0 then 0 else
            a.gsv/b.price*(1-pcp.percent) end as volume
            from lz_pdmr_adjusted_target a 
            left join iz_pdmr_product_price b 
		        on a.customer = b.customer and a.category_code = b.category_code  and a.`year` = b.`Year` 
            and a.period = b.period
            left join pdmr_customer_percent pcp 
            on a.customer = pcp.customer and  concat(a.period,'Y', SUBSTRING(a.`year`,3,2))  = pcp.period and a.statics_period = pcp.static_period 
		     where  a.customer = 'VET_EVET' and a.statics_period = :statics_period and a.Pvalue_Pyear>a.statics_period)a
            '''

            self.insert_or_update(sql, {'statics_period':current_P})


    def get_genery_file_info(self):
        params = dict()
        statement = '''
         select pillar ,p_year ,p_period,p_file from pdmr_genery_file 
         '''
        data = self.list(statement, params)
        return self.aggregate_data(data)

    def aggregate_data(self,data):
        result = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        for item in data:
            pillar = item['pillar']
            year = item['p_year']
            period = item['p_period']
            file = item['p_file']

            result[pillar][year][period].append(file)

        return result

    def update_sb(self,pillar):
        P = self.get_P_info()[0]['current_p']
        params = dict()
        params['pillar'] = pillar
        params['is_submit'] = 1
        params['static_period'] = P
        statement = '''
         insert into pdmr_submit  (pillar,is_submit,static_period) VALUES (:pillar, :is_submit, :static_period)
         ON DUPLICATE KEY UPDATE is_submit = 1;
         '''

        return self.insert_or_update(statement,params)
    def get_pdmr_genery_log(self):
        P = self.get_P_info()[0]['current_p']
        params = dict()
        params['static_period'] = P
        # statement = '''
        # select ka,'PDMR Generate：Completed' as message,create_dt  as `date` from user_action_log where `action` = 'PDMR Generate'
        # '''
        statement = '''
        select * from (
select pillar as `ka`,status as message,insert_dt  as `date`,uid,status_code  from pdmr_running_log where  static_period = :static_period
union all
select ka,'submit' as message,create_dt  as `date`,uid ,'' as status_code from user_action_log where `action` = 'submit'  and module = 'pdmr'
) a  order by date
        '''
        data =  self.list(statement,params)
        result = defaultdict(list)
        for item in data:
            ka = item['ka']
            message = item['message']
            date = item['date']
            uid = item['uid']
            status_code = item['status_code']
            result[ka].append({'message': message, 'date': date,'pillar':ka,'uid':uid,'status_code':status_code})
        result = dict(result)
        return result

    def pdmr_check(self,pillar):
        P = self.get_P_info()[0]['current_p']
        if pillar == 'OMNI_ALL':
            statement = f'''
            
select SUM(is_submit) =count(distinct pillar) as flag from pdmr_submit where static_period = '{P}'  and (pillar like 'OMNI%%' or pillar = 'DOUYIN')  
                    '''
        else :
            statement = f'''
                select SUM(is_submit) = count(distinct pillar) as flag from pdmr_submit where static_period = '{P}'   
                    '''
        return  self.list(statement, dict())

    def get_all_ka_log(self):
        statement = '''
   	        select p.Pillar,a.`SA Upload`,c.`MKT Upload`,b.`PDMR Generate`,d.`Submit` from
   				(select distinct ka_name as pillar  from wz_pdmr_mapping wpm where `type` = 1) p
   							  left join 	(   	
                select ka as  Pillar  ,max(create_dt) as 'SA Upload' from user_action_log   
                              where  action = 'SA Upload'   group by ka )a
                              on p.pillar = a.pillar
                              left join (
                select ka as  Pillar  ,max(create_dt) as 'PDMR Generate' from user_action_log   
                              where  action = 'PDMR Generate'  group by ka)b        
                              on p.pillar = b.pillar 
                              left join (
                select ka as  Pillar  ,max(create_dt) as 'MKT Upload' from user_action_log   
                              where   action = 'MKT Upload'     group by ka)c
                              on p.pillar = c.pillar
                              left join (
                 select ka as  Pillar  ,max(create_dt) as 'Submit' from user_action_log   
                              where  `action` = 'submit' group by ka)d
                              on p.pillar = d.pillar 
        '''

        return self.list(statement,dict())

    def get_running_status(self,pillar):
        params = dict()
        params['pillar'] = pillar
        statement = '''
        select *  from pdmr_running_log where pillar = :pillar  and status = 'running'     
        '''
        return self.list(statement,params)

    def genert_running_status(self,pillar,uid,current_p):
        params = dict()
        params['pillar'] = pillar
        params['uid'] = uid
        params['static_period'] = current_p
        statement = '''
        insert into pdmr_running_log (pillar,uid,status,static_period) values (:pillar,:uid,'running',:static_period)
        '''

        return self.insert_or_update(statement,params)
    def update_running_status(self,pillar,current_p,uid,status,status_code):
        params = dict()
        params['pillar'] = pillar
        params['uid'] = uid
        params['static_period'] = current_p
        params['status'] = status
        params['st_code'] = status_code
        statement = '''
        update pdmr_running_log set status = :status , status_code = :st_code where status = 'running' and uid = :uid and pillar = :pillar and static_period = :static_period;
        '''

        return self.insert_or_update(statement,params)

    def d2c_lz_to_iz(self, pillar, customer, current_P):
        params = dict()
        params['Pillar'] = pillar
        params['customer'] = customer
        params['statics_period'] = current_P

        first_sql = """
                delete from iz_pdmr_adjusted_target 
                where Pillar = :Pillar and customer = :customer and statics_period = :statics_period;
                """
        second_sql = '''
                    insert into iz_pdmr_adjusted_target
                    (select Pillar,
                    customer,
                    category_code,
                    bag_size,
                    animal_species,
                    gsv,
                    product_type,
                    volume,
                    `year`,
                    period,
                    Pvalue_Pyear,
                    insert_time,
                    statics_period
                    from lz_pdmr_adjusted_target
                    where  Pillar = :Pillar
                    and customer = :customer
                    and statics_period = :statics_period
                    and Pvalue_Pyear > :statics_period
                    )
                '''

        self.insert_or_update(first_sql, params)
        self.insert_or_update(second_sql, params)

    def get_latest_file(self):
        statement = '''
                SELECT 
                    t1.pillar, 
                    t1.p_year , 
                    t1.p_period , 
                    t1.insert_time, 
                    t1.p_file  
                FROM 
                    pdmr_genery_file t1
                INNER JOIN (
                    SELECT 
                        pillar , 
                        MAX(insert_time) AS insert_time
                    FROM 
                        pdmr_genery_file
                    GROUP BY 
                        pillar
                ) t2 
                ON 
                    t1.pillar = t2.pillar AND t1.insert_time = t2.insert_time;

        '''

        data = self.list(statement, dict())
        result = {item['pillar']: {k: v for k, v in item.items() if k != 'pillar'} for item in data}
        return result

    def get_submit_log(self):
        P = self.get_P_info()[0]['current_p']
        params = dict()
        params['static_period'] = P
        # statement = '''
        # select ka,'PDMR Generate：Completed' as message,create_dt  as `date` from user_action_log where `action` = 'PDMR Generate'
        # '''
        statement =    '''
             		     
		     select 
		     t1.ka,'submit' as message,
		     t1.create_dt  as `date`,
		     uid 
		     from user_action_log t1
		     inner join (
		        SELECT 
                        ka , 
                        MAX(create_dt) AS create_dt
                    FROM 
                        user_action_log
                        where `action` = 'submit' and module = 'Pdmr'
                    GROUP BY 
                        ka
		     ) t2
		     ON 
                    t1.ka = t2.ka AND t1.create_dt = t2.create_dt
		     where t1.`action` = 'submit' and module = 'Pdmr'
		     
        '''
        data = self.list(statement, params)
        result = defaultdict(list)
        for item in data:
            ka = item['ka']
            message = item['message']
            date = item['date']
            uid = item['uid']
            result[ka].append({'message': message, 'date': date, 'pillar': ka, 'uid': uid})
        result = dict(result)
        return result


    def get_input_flag(self):
        params = dict()

        statement = '''
           
                        select   current_period   from pdmr_input_ctl  order by insert_dt desc limit 1;
                    '''

        data =  self.list(statement, params)
        A_P = data[0]['current_period']
        B_P = self.get_P_info()[0]['current_p']
        return A_P == B_P
        #TODO
        # return True

    def get_all_ka_name(self):
        params = dict()

        statement = '''

                        select distinct ka_name as pillar  from wz_pdmr_mapping wpm where `type` = 1
                    '''

        data = self.list(statement, params)
        return data
    ab 