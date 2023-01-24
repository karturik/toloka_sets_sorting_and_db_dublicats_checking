import pandas as pd
import toloka.client as toloka
import os
import time
import psycopg2
from colorama import Fore, Style
import shutil
import hashlib
import datetime

validator_name = 'Validator name'

e = datetime.datetime.now()
date = f"{'%s_%s_%s' % (e.year, e.month, e.day)}"

# DATABASE CONNECTION
conn = psycopg2.connect("""
    host=host
    port=port
    sslmode=require
    dbname=dbname
    user=user
    password=password
    target_session_attrs=read-write
""")
q = conn.cursor()

# CREATE DIRS
if not os.path.exists('sorted_sets'):
    os.mkdir('sorted_sets')

if not os.path.exists('new_sets'):
    os.mkdir('new_sets')


# TOLOKA PARAMS
URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
account = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')


# HASHES CREATE FUNCTION
def hash_create(full_df: pd.DataFrame) -> pd.DataFrame:
    df_with_all_files = pd.DataFrame()
    for assignment_id in full_df['assignment_id'].unique():
        df_one_set = full_df[full_df['assignment_id'] == assignment_id]
        set_hashes = {}
        for file_name in df_one_set['file_name']:
            set_hashes[file_name] = df_one_set[df_one_set['file_name']==file_name]['hash'].values[0]
        tmp_df = pd.DataFrame(data={'assignment_id':[assignment_id], 'files_hashes':[set_hashes]})
        df_with_all_files = pd.concat([df_with_all_files, tmp_df])
    return df_with_all_files


# FUNCTION FOR ALL SET DATA FROM TOLOKA
def get_toloka_data(set_id: str, toloka_client: toloka.TolokaClient, account: str, OAUTH_TOKEN: str) -> {str: str}:
    success = False
    tries = 0
    while success != True:
        try:
            set_data = toloka_client.get_assignment(assignment_id=set_id)
            worker_id = set_data.user_id
            pool_id = set_data.pool_id
            project_id = toloka_client.get_pool(pool_id=pool_id).project_id
            reward = set_data.reward
            df_toloka = toloka_client.get_assignments_df(pool_id, status=['APPROVED', 'SUBMITTED', 'REJECTED'])
            assignment_link = f'https://platform.toloka.ai/requester/project/{project_id}/pool/{pool_id}/assignments/{set_id}?direction=ASC'
            success = True
            pool_data = toloka_client.get_pool(pool_id=pool_id)
            pool_name = pool_data.private_name
            if 'new' in pool_name.lower() and not 'retry' in pool_name.lower() and not 'родствен' in pool_name.lower():
                pool_type = 'new'
            elif 'retry' in pool_name.lower():
                pool_type = 'retry'
            elif 'родствен' in pool_name.lower():
                pool_type = 'родственники'
            else:
                pool_type = ''
            all_set_data = {'worker_id': worker_id, 'pool_id': pool_id,
                            'project_id': project_id,'assignment_link': assignment_link,
                            'df_toloka': df_toloka, 'account': account,
                            'pool_type':pool_type, 'reward':reward}
            return all_set_data
        # WORKING WITH TWO ACCOUNTS
        except Exception as e:
            if 'DoesNotExistApiError' in str(e):
                if OAUTH_TOKEN == '':
                    OAUTH_TOKEN = ''
                    account = ''
                elif OAUTH_TOKEN == '':
                    OAUTH_TOKEN = ''
                    account = ''
                HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
                toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')
                tries += 1
            else:
                print(f'Error, try {tries}/10')
                tries += 1
                time.sleep(1)
            if tries == 10:
                with open('errors.tsv', 'a', encoding='utf-8') as file:
                    file.write(f"{set_id}\t{e}\n")
                    file.close()
                success = True

new_sets_for_check = []

# CHECKING SETS WITH DATABSE
for root, subdirectories, files in os.walk('new_sets'):
    for subdirectory in subdirectories:
        q.execute('''SELECT assignment_id, worker_id, assignment_nation, assignment_month,
                                                          assignment_send_date, assignment_toloka_date, toloka_status,
                                                          reward, account, pool_type, decision, reject_reason,
                                                          hashes, gender FROM public.sets ''')
        all_sets_in_db_df = pd.DataFrame(q.fetchall(),
                                         columns=['assignment_id', 'worker_id', 'assignment_nation',
                                                  'assignment_month', 'assignment_send_date',
                                                  'assignment_toloka_date', 'toloka_status', 'reward',
                                                  'account', 'pool_type', 'decision',
                                                  'reject_reason', 'hashes', 'gender'])

        set_id = os.path.join(root, subdirectory).split('\\')[-1]
        # print(set_id)
        all_main_set_data = get_toloka_data(set_id, toloka_client, account, OAUTH_TOKEN)
        main_assignment_link = all_main_set_data['assignment_link']
        print('Link to checking set: ', main_assignment_link)

        for file in os.listdir(os.path.join(root, subdirectory)):
            if 'metadata' in file.lower():
                tries = 0
                while tries < 10:
                    try:
                        path = os.path.join(root, subdirectory)
                        main_worker_id = all_main_set_data['worker_id']
                        main_pool_id = all_main_set_data['pool_id']
                        main_project_id = all_main_set_data['project_id']
                        main_assignment_link = all_main_set_data['assignment_link']
                        main_df_toloka = all_main_set_data['df_toloka']
                        main_account = all_main_set_data['account']
                        main_pool_type = all_main_set_data['pool_type']
                        main_toloka_date = main_df_toloka[main_df_toloka['ASSIGNMENT:assignment_id'] == set_id]['ASSIGNMENT:started'].values[0].split('T')[0]
                        main_toloka_status = main_df_toloka[main_df_toloka['ASSIGNMENT:assignment_id'] == set_id]['ASSIGNMENT:status'].values[0]
                        main_reward = all_main_set_data['reward']
                        metadata_df = pd.read_excel(f'{os.path.join(path, file)}', sheet_name='Sheet1')
                        ethnicity = metadata_df['Unnamed: 1'][1]
                        set_with_same_worker = all_sets_in_db_df[all_sets_in_db_df['worker_id'] == main_worker_id]
                        if not set_with_same_worker.empty:
                            print(Fore.RED + 'There are same workers sets in DB: ' + Style.RESET_ALL)
                            for set in set_with_same_worker['assignment_id']:
                                assignment_id = set
                                all_set_data = get_toloka_data(assignment_id, toloka_client, account, OAUTH_TOKEN)
                                pool_id = all_set_data['pool_id']
                                project_id = all_set_data['project_id']
                                assignment_link = all_set_data['assignment_link']
                                df_toloka = all_set_data['df_toloka']
                                account = all_set_data['account']
                                pool_type = all_set_data['pool_type']
                                decision = set_with_same_worker[set_with_same_worker['assignment_id']==assignment_id]['decision'].values[0]
                                reject_reason = set_with_same_worker[set_with_same_worker['assignment_id']==assignment_id]['reject_reason'].values[0]
                                print('Same worker sets in DB information: ')
                                print('     ', assignment_link)
                                print(Fore.RED + f"    ({assignment_id} {Style.RESET_ALL},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['worker_id'].values[0]},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['assignment_nation'].values[0]},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['assignment_month'].values[0]},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['assignment_send_date'].values[0]},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['assignment_toloka_date'].values[0]},"
                                                 f"{Fore.RED} toloka-status: {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['toloka_status'].values[0]}{Style.RESET_ALL},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['pool_type'].values[0]},"
                                                 f" {Fore.RED} status: {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['decision'].values[0]} {Style.RESET_ALL},"
                                                 f" {all_sets_in_db_df[all_sets_in_db_df['assignment_id']==assignment_id]['reject_reason'].values[0]})" + Style.RESET_ALL)

                                if set_id == assignment_id:
                                    print(Fore.GREEN + 'Sets have same assignment_id - we have that set, it can be deleted' + Style.RESET_ALL)
                                if pool_type == 'родственники':
                                    print(Fore.YELLOW + 'That set with RELATIVES' + Style.RESET_ALL)
                                    if decision == 'ACCEPTED':
                                        print(Fore.GREEN + 'This set already was accepted, if here we have same person - it can be deleted' + Style.RESET_ALL)
                                    elif decision == 'IN WORK':
                                        print(Fore.GREEN + 'This set in work, if here we have same person - it can be deleted' + Style.RESET_ALL)
                                    elif 'LOCAL' in decision:
                                        print(Fore.GREEN + 'This set local on validators PC' + Style.RESET_ALL)
                                    elif decision == 'REJECTED':
                                        if reject_reason and reject_reason != 'None':
                                            print(Fore.GREEN + f'This set with relatives was rejected, if here same person - check errors and it can be passed \n Last errors: {set[11]}' + Style.RESET_ALL)
                                        else:
                                            print(Fore.GREEN + 'This set with relatives was rejected, if here same person - check errors and it can be passed' + Style.RESET_ALL)
                                elif pool_type == 'retry':
                                    print(Fore.YELLOW + 'This set from pool retry, here can be some corrected errors' + Style.RESET_ALL)
                                    if decision == 'ACCEPTED':
                                        print(Fore.GREEN + 'This set already was accepted, we dont need that' + Style.RESET_ALL)
                                    elif decision == 'REJECTED':
                                        if reject_reason and reject_reason != 'None':
                                            print(Fore.GREEN + f'This set from pool retry, here can be some corrected errors \n Last errors: {set[11]}' + Style.RESET_ALL)
                                        else:
                                            print(Fore.GREEN + 'This set from pool retry, here can be some corrected errors' + Style.RESET_ALL)
                                    elif 'LOCAL' in decision:
                                        print(Fore.GREEN + 'This set local on validators PC' + Style.RESET_ALL)
                                    elif decision == 'IN WORK':
                                        print(Fore.GREEN + 'This set in work, if here we have same person - it can be deleted' + Style.RESET_ALL)
                                else:
                                    if decision == 'ACCEPTED':
                                        print(Fore.GREEN + 'This set already was accepted, we dont need that' + Style.RESET_ALL)
                                    elif decision == 'REJECTED':
                                        if reject_reason and reject_reason != 'None':
                                            print(Fore.GREEN + f'This set was rejected, here can be some corrected errors, but not from pool retry\n Last errors: {set[11]}' + Style.RESET_ALL)
                                        else:
                                            print(Fore.GREEN + 'This set was rejected, here can be some corrected errors, but not from pool retry' + Style.RESET_ALL)
                                    elif 'LOCAL' in decision:
                                        print(Fore.GREEN + 'This set local on validators PC' + Style.RESET_ALL)
                                    elif decision == 'IN WORK':
                                        print(Fore.GREEN + 'This set in work, if here we have same person - it can be deleted' + Style.RESET_ALL)
                                print('----------------------------------------------------------------------------------------------------------------')
                            # VALIDATORS DECISION AFTER INFORMATION FROM DATABASE
                            acceptable_decision = False
                            while acceptable_decision != True:
                                decision = input('What we are doing? \n 1.All right, upload and insert in DB \n 2.Upload to retry dir \n 3.Leave that set, manual process \n 4.Delete set from PC\n')
                                if decision == '1' or decision == '2' or decision == '3' or decision == '4':
                                    acceptable_decision = True
                                else:
                                    print('Invalid decision')
                        else:
                            decision = '1'
                        if decision == '1':
                            if all_sets_in_db_df[all_sets_in_db_df['assignment_id'] == set_id].empty:
                                q.execute(f"INSERT INTO public.sets (assignment_id, worker_id, assignment_nation,"
                                          f" assignment_toloka_date, toloka_status, reward, account, pool_type,"
                                          f" decision, reject_reason, assignment_month, assignment_send_date,"
                                          f" validator_name, validation_date) VALUES ('{set_id}', '{main_worker_id}',"
                                          f" '{ethnicity}', '{main_toloka_date}', '{main_toloka_status}', '{main_reward}',"
                                          f" '{main_account}', '{main_pool_type}', 'LOCAL', 'None', 'None', 'None', '{validator_name}', '{date}')")
                                conn.commit()
                                print('Add to DB!')
                            else:
                                print('There is same assignment_id in DB, dont add again')
                            if not os.path.exists(f'sorted_sets/{ethnicity}'):
                                os.makedirs(f'sorted_sets/{ethnicity}')
                            shutil.move(os.path.join(path, file).replace('metadata.xlsx', ''), f'sorted_sets/{ethnicity}')
                            print('Move to correct ethnicity')
                            new_sets_for_check.append(set_id)
                        elif decision == '2':
                            if not os.path.exists(f'sorted_sets/retry/{ethnicity}'):
                                os.makedirs(f'sorted_sets/retry/{ethnicity}')
                            shutil.move(os.path.join(path, file).replace('metadata.xlsx', ''), f'sorted_sets/retry/{ethnicity}')
                            print('Move to retry')
                            new_sets_for_check.append(set_id)
                        elif decision == '3':
                            print('Skipp set')
                        elif decision == '4':
                            shutil.rmtree(os.path.join(path, file).replace('metadata.xlsx', ''))
                            print('Delete set')
                        print('===================================================================================================')
                        tries = 100
                    except Exception as e:
                        print(e)
                        time.sleep(1)
                        tries += 1
                        if tries == 10:
                            with open('errors.tsv', 'a', encoding='utf-8') as file:
                                try:
                                    file.write(f"{set_id}\t{e}\n")
                                except:
                                    file.write(f"{e}\n")
                                file.close()

# TMP DATAFRAMES
full_df = pd.DataFrame()
full_df1 = pd.DataFrame()

print(new_sets_for_check)


# CREATE AND COMPARE HASHES WITH HASHES IN DATABASE
for root, subdirectories, files in os.walk('sorted_sets'):
    q.execute('''SELECT assignment_id, worker_id, assignment_nation, assignment_month,
                                              assignment_send_date, assignment_toloka_date, toloka_status,
                                              reward, account, pool_type, decision, reject_reason,
                                              hashes, gender FROM public.sets ''')
    all_sets_in_db_df = pd.DataFrame(q.fetchall(),
                                     columns=['assignment_id', 'worker_id', 'assignment_nation', 'assignment_month',
                                              'assignment_send_date', 'assignment_toloka_date', 'toloka_status',
                                              'reward', 'account', 'pool_type', 'decision', 'reject_reason',
                                              'hashes', 'gender'])

    all_sets_in_db_df = all_sets_in_db_df.dropna(subset=['hashes'])
    work_dict = {}
    decision = '1'
    for subdirectory in subdirectories:
        path = os.path.join(root, subdirectory)
        if len(path.split('\\')) > 2:
            assignment_id = path.split('\\')[-1]
            # print(assignment_id)
            if assignment_id in new_sets_for_check:
                assignment_link = get_toloka_data(assignment_id,  toloka_client, account, OAUTH_TOKEN)['assignment_link']
                print('Link to check set by hashes: ', assignment_link)
                for file in os.listdir(os.path.join(root, subdirectory)):
                    file_name = os.path.join(path, file).split('\\')[-1]
                    hash = hashlib.md5(open(os.path.join(path, file), 'rb').read()).hexdigest()
                    df = pd.DataFrame(data={'assignment_id': [assignment_id], 'file_name': [file_name], 'hash': [hash]})
                    full_df = pd.concat([full_df, df])
                if not full_df.empty:
                    # print(full_df)
                    full_df1 = hash_create(full_df)
                    work_dict = full_df1['files_hashes'].values[0]
                    # print(work_dict)
                    df_with_dublicated_sets = pd.DataFrame()
                    for key, value in work_dict.items():
                        # print(value)
                        if not all_sets_in_db_df[all_sets_in_db_df['hashes'].str.contains(value)].empty:
                            # dublicated_hash = value
                            df_with_dublicated_sets = pd.concat([df_with_dublicated_sets, all_sets_in_db_df[all_sets_in_db_df['hashes'].str.contains(value)]])
                    if not df_with_dublicated_sets.empty:
                        # print(df_with_dublicated_sets)
                        for dublicated_set in df_with_dublicated_sets['assignment_id'].drop_duplicates():
                            # print(f"У сета {assignment_id} дубликаты по хешу с сетом: {dublicated_set}")
                            if dublicated_set != assignment_id:
                                assignment_link_1 = get_toloka_data(dublicated_set, toloka_client, account, OAUTH_TOKEN)['assignment_link']
                                print('Link to set with same hashes: ', assignment_link_1)
                                decision = input('What we are doing? \n 1.All right, save set and add hashes to DB \n 2.Delete set')
                                if decision == '1':
                                    work_dict = str(work_dict).replace("'", '"')
                                    q.execute(f"UPDATE public.sets SET hashes = '{work_dict}' WHERE assignment_id = '{assignment_id}';")
                                    conn.commit()
                                    print(f'Add to DB for set {assignment_id} hashes: {work_dict}')
                                if decision == '2':
                                    shutil.rmtree(path)
                                    print(f'Delete from PC {assignment_id} with hashes: {work_dict}')

                if len(work_dict) > 0 and decision != '2':
                    work_dict = str(work_dict).replace("'", '"')
                    q.execute(f"UPDATE public.sets SET hashes = '{work_dict}' WHERE assignment_id = '{assignment_id}';")
                    conn.commit()
                    print(f'Add to DB for {assignment_id} hashes: {work_dict}')
                    print('----------------------------------------------------')

            full_df = pd.DataFrame()

conn.close()