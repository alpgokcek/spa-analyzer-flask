import pandas as pd
from collections import defaultdict
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import datetime
from dotenv import load_dotenv
from os import environ
import threading
from ThreadWithReturn import ThreadWithReturnValue

load_dotenv()


def get_result_proxy(resultproxy):
    for rowproxy in resultproxy:
        # rowproxy.items() returns an array like [(key0, value0), (key1, value1)]
        for column, value in rowproxy.items():
            return value


def get_result_proxy_list(resultproxy, query='id'):
    if query == 'id':
        output = []
        for rowproxy in resultproxy:
            # rowproxy.items() returns an array like [(key0, value0), (key1, value1)]
            output.append(rowproxy[query])
    else:
        output = dict()
        for rowproxy in resultproxy:
            # rowproxy.items() returns an array like [(key0, value0), (key1, value1)]
            output[rowproxy[query]] = rowproxy['id']
    return output


def nested_defaultdict():
    return defaultdict(nested_defaultdict)


program_outcomes, course_outcomes, student_list, exams, grading_tool_grades = dict(), dict(), list(), defaultdict(
    nested_defaultdict), list()
course_code, course_name, course_credit, course_year_and_term, course_department, course_section = '', '', '', '', '', ''
program_outcomes_id, course_outcomes_id, course_id, student_id, exam_id, grading_tool_id = dict(), dict(), 0, dict(), dict(), defaultdict(
    list)


DB_USER, DB_PASS, DB_HOST, DB_PORT, DATABASE = environ.get('DB_USER'), environ.get('DB_PASS'), environ.get(
    'DB_HOST'), environ.get('DB_PORT'), environ.get('DATABASE')

# create sqlalchemy engine
connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DATABASE)

engine = create_engine(connect_string, poolclass=NullPool)


def clear_global_variables():
    global program_outcomes, course_outcomes, student_list, exams, grading_tool_grades, course_code, course_name, \
        course_credit, program_outcomes_id, course_outcomes_id, course_id, student_id, exam_id, grading_tool_id

    program_outcomes, course_outcomes, student_list, exams, grading_tool_grades = dict(), dict(), list(), defaultdict(
        nested_defaultdict), list()
    course_code, course_name, course_credit = 'IE201', 'Industrial Engineering Test', 6
    program_outcomes_id, course_outcomes_id, course_id, student_id, exam_id, grading_tool_id = dict(), dict(), 0, dict(), dict(), defaultdict(
        list)


def delete_excel(section, department, code, year_and_term, name, credit):
    conn = engine.connect()
    assessments_to_delete, grading_tools_to_delete, course_outcomes_to_delete = [], [], []
    metadata = sqlalchemy.MetaData()
    grading_tool_table = sqlalchemy.Table('grading_tool', metadata, autoload=True, autoload_with=engine)
    assessment_table = sqlalchemy.Table('assessment', metadata, autoload=True, autoload_with=engine)
    course_outcome_table = sqlalchemy.Table('course_outcome', metadata, autoload=True, autoload_with=engine)
    po_provides_co_table = sqlalchemy.Table('program_outcomes_provides_course_outcomes', metadata, autoload=True,
                                            autoload_with=engine)
    student_answers_gt_table = sqlalchemy.Table('student_answers_grading_tool', metadata, autoload=True,
                                                autoload_with=engine)
    gt_covers_co_table = sqlalchemy.Table('grading_tool_covers_course_outcome', metadata, autoload=True,
                                          autoload_with=engine)
    student_get_grade_co_table = sqlalchemy.Table('student_gets_measured_grade_course_outcome', metadata, autoload=True,
                                          autoload_with=engine)
    student_get_grade_po_table = sqlalchemy.Table('student_gets_measured_grade_program_outcome', metadata, autoload=True,
                                                  autoload_with=engine)

    def po_provides_co():
        print("- Started: Deleting program_outcomes_provides_course_outcomes")
        inner_conn = engine.connect()
        for i in course_outcomes_to_delete:
            delete_query = po_provides_co_table.delete().where(po_provides_co_table.c.course_outcome_id == i)
            inner_conn.execute(delete_query)
        inner_conn.close()
        print("+ Done: Deleting program_outcomes_provides_course_outcomes")

    def student_answers_grading_tool_delete():
        print("- Started: Deleting student_answers_grading_tool")
        inner_conn = engine.connect()
        for i in grading_tools_to_delete:
            delete_query = student_answers_gt_table.delete().where(student_answers_gt_table.c.grading_tool_id == i)
            inner_conn.execute(delete_query)
        inner_conn.close()
        print("+ Done: Deleting student_answers_grading_tool")

    def gt_covers_co():
        print("- Started: Deleting grading_tool_covers_course_outcome")
        inner_conn = engine.connect()
        for i in course_outcomes_to_delete:
            delete_query = gt_covers_co_table.delete().where(gt_covers_co_table.c.course_outcome_id == i)
            inner_conn.execute(delete_query)
        inner_conn.close()
        print("+ Done: Deleting grading_tool_covers_course_outcome")

    def student_get_grade_co():
        print("- Started: Deleting student_gets_measured_grade_course_outcome")
        inner_conn = engine.connect()
        for i in course_outcomes_to_delete:
            delete_query = student_get_grade_co_table.delete().where(student_get_grade_co_table.c.course_outcome_id == i)
            inner_conn.execute(delete_query)
        inner_conn.close()
        print("+ Done: Deleting student_gets_measured_grade_course_outcome")

    def student_get_grade_po():
        print("- Started: Deleting student_gets_measured_grade_program_outcome")
        inner_conn = engine.connect()
        delete_query = student_get_grade_po_table.delete().where(student_get_grade_po_table.c.course_id == course_id)
        inner_conn.execute(delete_query)
        inner_conn.close()
        print("+ Done: Deleting student_gets_measured_grade_course_outcome")

    def delete_rest():
        print("- Started: Deleting rest")
        inner_conn = engine.connect()
        for i in assessments_to_delete:
            delete_query = grading_tool_table.delete().where(grading_tool_table.c.assessment_id == i)
            inner_conn.execute(delete_query)
            delete_query = assessment_table.delete().where(assessment_table.c.id == i)
            inner_conn.execute(delete_query)
        for i in course_outcomes_to_delete:
            delete_query = course_outcome_table.delete().where(course_outcome_table.c.id == i)
            inner_conn.execute(delete_query)
        inner_conn.close()
        print("+ Done: Deleting rest")

    course_table = sqlalchemy.Table('course', metadata, autoload=True, autoload_with=engine)
    course_query = sqlalchemy.select([course_table]).where(
        sqlalchemy.and_(course_table.columns.department_id == department, course_table.columns.code == code,
                        course_table.columns.year_and_term == year_and_term, course_table.columns.title == name,
                        course_table.columns.credit == credit))
    course_id = get_result_proxy(conn.execute(course_query))
    ####

    section_table = sqlalchemy.Table('section', metadata, autoload=True, autoload_with=engine)
    section_query = section_table.update().values(is_file_uploaded=0).where(
        section_table.columns.id == section)
    conn.execute(section_query)



    print("- Started: Assessment ID fetch")
    # Assessments
    assessment_select_query = sqlalchemy.select([assessment_table]).where(
        assessment_table.columns.course_id == course_id)
    assessments_to_delete = get_result_proxy_list(conn.execute(assessment_select_query))
    print("+ Done: Assessment ID fetch")

    # Grading Tool
    print("- Started: Grading Tool ID fetch")
    for assessment_id in assessments_to_delete:
        grading_tool_delete_query = sqlalchemy.select([grading_tool_table]).where(
            grading_tool_table.columns.assessment_id == assessment_id)
        grading_tools_to_delete += get_result_proxy_list(conn.execute(grading_tool_delete_query))
    print("+ Done: Grading Tool ID's fetch")

    # Course Outcome
    print("- Started: Course Outcome ID fetch")
    course_outcome_select_query = sqlalchemy.select([course_outcome_table]).where(
        course_outcome_table.columns.course_id == course_id)
    course_outcomes_to_delete = get_result_proxy_list(conn.execute(course_outcome_select_query))
    print("+ Done: Course Outcome ID fetch")

    threads = [threading.Thread(target=po_provides_co), threading.Thread(target=student_answers_grading_tool_delete),
               threading.Thread(target=gt_covers_co), threading.Thread(target=student_get_grade_co)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    delete_rest()
    conn.close()
    engine.dispose()
    return True





def start_threads(section, department, code, year_and_term, name, credit):
    global course_id
    print("- Started:  Course")
    try:
        conn = engine.connect()
        metadata = sqlalchemy.MetaData()
        course_table = sqlalchemy.Table('course', metadata, autoload=True, autoload_with=engine)
        course_query = sqlalchemy.select([course_table]).where(
            sqlalchemy.and_(course_table.columns.department_id == department, course_table.columns.code == code,
                            course_table.columns.year_and_term == year_and_term, course_table.columns.title == name,
                            course_table.columns.credit == credit))
        course_id = get_result_proxy(conn.execute(course_query))

        print(course_id, section, department, code, year_and_term, name, credit)
        if course_id is None:
            print(course_id is None)
            return 'Course not found'
    except Exception as e:
        return e
    section_table = sqlalchemy.Table('section', metadata, autoload=True, autoload_with=engine)
    section_query = section_table.update().values(is_file_uploaded=1).where(
        section_table.columns.id == section)
    conn.execute(section_query)
    print("+ Done: Course")

    thread1 = ThreadWithReturnValue(target=program_outcomes_course_outcomes)
    thread1.start()
    print("program_outcomes_course_outcomes", thread1.join())

    thread2 = ThreadWithReturnValue(target=grading_tool_and_assessments)
    thread2.start()
    print("program_outcomes_course_outcomes", thread2.join())

    # conn1 = engine.raw_connection()
    # curs = conn1.cursor()
    # curs.callproc('calc_cos_for_course', [course_id])
    # curs.callproc('calc_pos', [course_id])
    # curs.close()
    # conn1.commit()
    # conn1.close()

    conn.close()
    engine.dispose()
    return True

###################################################################################################
############################### COURSE OUTCOMES - PROGRAM OUTCOMES ################################
###################################################################################################
def program_outcomes_course_outcomes():
    global program_outcomes_id
    conn = engine.connect()
    metadata = sqlalchemy.MetaData()
    program_outcome_table = sqlalchemy.Table('program_outcome', metadata, autoload=True, autoload_with=engine)
    program_outcome_select_query = sqlalchemy.select([program_outcome_table]).where(
        sqlalchemy.and_(program_outcome_table.columns.year_and_term == course_year_and_term,
                        program_outcome_table.columns.department_id == course_department))

    program_outcomes_id = get_result_proxy_list(conn.execute(program_outcome_select_query), 'code')
    print('- Started: program outcomes course outcomes')
    try:
        for i in course_outcomes['Course Outcome Explanation'].keys():
            now = datetime.datetime.utcnow()

            course_outcome = pd.DataFrame(
                {'course_id': course_id, 'explanation': course_outcomes['Course Outcome Explanation'][i], 'code': i,
                 'created_at': now, 'updated_at': now},
                index=[0])


            course_outcome.to_sql('course_outcome', con=conn, if_exists='append', chunksize=1000, index=False)

            last_id = conn.execute("SELECT LAST_INSERT_ID();")

            course_outcomes_id[i] = get_result_proxy(last_id)

    except Exception as e:
        return e
    conn.close()

    print('+ Done: program outcomes course outcomes')

    thread1 = ThreadWithReturnValue(target=course_outcome_provides_program_outcome)
    thread1.start()
    output = thread1.join()
    print(output)
    if output is not True: return output
    return True


###################################################################################################
############################ COURSE OUTCOMES PROVIDES PROGRAM OUTCOMES ############################
###################################################################################################
def course_outcome_provides_program_outcome():
    conn = engine.connect()
    course_outcome_provides_program_outcome_dataframe = pd.DataFrame()
    print("- Started: program outcomes provides course outcomes")
    try:
        for i in course_outcomes['Program Outcomes'].keys():
            now = datetime.datetime.utcnow()
            splitted_program_outcomes = [x.strip() for x in course_outcomes['Program Outcomes'][i].split(',')]
            for j in splitted_program_outcomes:
                course_outcome_provides_program_outcome_dataframe = course_outcome_provides_program_outcome_dataframe.append(
                    pd.DataFrame(
                        {'course_outcome_id': course_outcomes_id[i], 'program_outcome_id': program_outcomes_id[j],
                         'created_at': now, 'updated_at': now}, index=[0]))
        course_outcome_provides_program_outcome_dataframe.to_sql('program_outcomes_provides_course_outcomes', con=conn,
                                                                 if_exists='append', chunksize=1000, index=False)
    except Exception as e:
        return e
    conn.close()
    print('+ Done: program outcomes provides course outcomes')
    return True


###################################################################################################
################################## GRADING TOOL and ASSESSMENTS ###################################
###################################################################################################

def grading_tool_and_assessments():
    conn = engine.connect()
    print("- Started: grading tool and assessments")
    for exam in exams.keys():
        now = datetime.datetime.utcnow()
        exam_percentage = exams[exam]['Exam Percentage']
        assessment = pd.DataFrame(
            {'name': exam, 'percentage': exam_percentage, 'course_id': course_id, 'created_at': now, 'updated_at': now},
            index=[0])
        assessment.to_sql('assessment', con=conn, if_exists='append', chunksize=1000, index=False)
        last_id = conn.execute("SELECT LAST_INSERT_ID();")
        exam_id[exam] = get_result_proxy(last_id)

    for exam in exams.keys():
        for question in exams[exam]['Questions'].keys():
            now = datetime.datetime.utcnow()
            grading_tool = pd.DataFrame({'assessment_id': exam_id[exam],
                                         'percentage': exams[exam]['Questions'][question]['Question Percentage'],
                                         'question_number': question, 'created_at': now, 'updated_at': now}, index=[0])
            grading_tool.to_sql('grading_tool', con=conn, if_exists='append', chunksize=1000, index=False)
            last_id = conn.execute("SELECT LAST_INSERT_ID();")
            grading_tool_id[exam].append(get_result_proxy(last_id))
    conn.close()
    print('+ Done: grading tool and assessments')

    thread1 = ThreadWithReturnValue(target=grading_tool_covers_course_outcome)
    thread1.start()
    output1 = thread1.join()
    print(output1)

    thread2 = ThreadWithReturnValue(target=student_answers_grading_tool)
    thread2.start()
    output2 = thread2.join()
    print(output2)

    if output1 is not True: return output1
    if output2 is not True: return output2
    return True


###################################################################################################
############################### GRADING TOOL COVERS COURSE OUTCOME ################################
###################################################################################################
def grading_tool_covers_course_outcome():
    conn = engine.connect()
    course_outcome_dataframe = pd.DataFrame()
    print("- Started: grading tool covers course outcome")
    try:
        for exam in exams.keys():
            now = datetime.datetime.utcnow()
            for i in range(len(exams[exam]['Questions'].keys())):
                question = list(exams[exam]['Questions'].keys())[i]
                splitted_course_outcomes = [x.strip() for x in
                                            exams[exam]['Questions'][question]['Related Outcomes'].split(',')]
                for co in splitted_course_outcomes:
                    course_outcome_dataframe = course_outcome_dataframe.append(pd.DataFrame(
                        {'grading_tool_id': grading_tool_id[exam][i], 'course_outcome_id': course_outcomes_id[co],
                         'created_at': now, 'updated_at': now}, index=[0]))
        course_outcome_dataframe.to_sql('grading_tool_covers_course_outcome', con=conn, if_exists='append',
                                        chunksize=1000,
                                        index=False)
    except Exception as e:
        return e
    conn.close()
    print('+ Done: grading tool covers course outcome')
    return True


###################################################################################################
################################## STUDENT ANSWERS GRADING TOOL ###################################
###################################################################################################

def student_answers_grading_tool():
    conn = engine.connect()
    sagt = pd.DataFrame()
    print("- Started: student answers grading tool")
    try:
        last_pos=0
        for exam_pos in range(len(exams)):
            exam = list(exams)[exam_pos]
            grading_tool_pos = 0
            for i in range(last_pos, len(exams[exam]['Questions'])+last_pos):
                for j in range(len(grading_tool_grades[i])):
                    now = datetime.datetime.utcnow()
                    # print(student_list)
                    sagt = sagt.append(pd.DataFrame(
                        {'student_id': student_list[j],
                         'grading_tool_id': grading_tool_id[exam][grading_tool_pos],
                         'grade': grading_tool_grades[i][j],
                         'created_at': now,
                         'updated_at': now},
                        index=[0]))
                grading_tool_pos = grading_tool_pos + 1
                last_pos = last_pos + 1
        # print("a")
        sagt.to_sql('student_answers_grading_tool', con=conn, if_exists='append', chunksize=1000, index=False)
        # print("b")
    except Exception as e:
        return e
    conn.close()
    print('+ Done: student answers grading tool')
    return True


def analyze_spa(file_path, section, department, code, year_and_term, name, credit):
    global course_code, course_name, course_credit, course_year_and_term, course_department, course_section
    course_code, course_name, course_credit, course_year_and_term, course_department, course_section = code, name, credit, year_and_term, department, section
    clear_global_variables()
    spa_program_sheet(file_path)
    spa_course_sheet(file_path)
    spa_grades_sheet(file_path)
    return start_threads(section, department, code, year_and_term, name, credit)


def spa_program_sheet(file_path):
    global program_outcomes
    data = pd.read_excel(file_path, sheet_name=0, skiprows=1)
    df = pd.DataFrame(data, columns=['Program Outcomes', 'Program Outcome Explanation'])
    program_outcomes = df.set_index('Program Outcomes')['Program Outcome Explanation'].to_dict()


def spa_course_sheet(file_path):
    global course_outcomes, course_code, course_name, course_credit
    data = pd.read_excel(file_path, sheet_name=1, skiprows=5, index_col='Course Outcomes')
    df = pd.DataFrame(data, columns=['Course Outcome Explanation', 'Program Outcomes'])
    course_outcomes = df.to_dict()
    data = pd.read_excel(file_path, sheet_name=1, header=None)
    df = pd.DataFrame(data)
    course_code, course_name, course_credit = df.iat[0, 1], df.iat[1, 1], df.iat[2, 1]


def spa_grades_sheet(file_path):
    global course_outcomes, course_code, course_name, course_credit, exams, student_list
    data = pd.read_excel(file_path, sheet_name=2, skiprows=9, header=None)
    df = pd.DataFrame(data)

    def students():
        global student_list, course_section
        # student id's
        temp_student_list = data.iloc[0:, 0].ravel().tolist()
        conn = engine.connect()
        print("- Started: checking student list")
        metadata = sqlalchemy.MetaData()
        students_takes_sections_table = sqlalchemy.Table('students_takes_sections', metadata, autoload=True,
                                                         autoload_with=engine)
        missing_students = []
        for i in temp_student_list:
            students_takes_sections_query = sqlalchemy.select([students_takes_sections_table]).where(
                sqlalchemy.and_(students_takes_sections_table.columns.student_id == i,
                                students_takes_sections_table.columns.section_id == course_section))
            students_takes_sections = get_result_proxy(conn.execute(students_takes_sections_query))
            if students_takes_sections:
                student_list.append(i)
            else:
                missing_students.append(i)


        for i in missing_students:
            print(i)
            now = datetime.datetime.utcnow()
            '''
            sts = pd.DataFrame(
                {'course_id': course_id, 'explanation': course_outcomes['Course Outcome Explanation'][i], 'code': i,
                 'created_at': now, 'updated_at': now},
                index=[0])

            sts.to_sql('student_takes_sections', con=conn, if_exists='append', chunksize=1000, index=False)

            last_id = conn.execute("SELECT LAST_INSERT_ID();")

            student_list[i] = get_result_proxy(last_id)
            '''



        conn.close()
        print('+ Done: checking student list')

    thread1 = threading.Thread(target=students)
    thread1.start()
    # each students grade on i'th question/column
    for i in range(5, df.shape[1]):
        grading_tool_grades.append(df[i][:].tolist())

        '''
       Following part of code creates a dictionary as the template below.
       {
          "[Exam Name]":{
             "Exam Percentage:":int,
             "Questions":{
                "[Question Name]":{
                   "Question Percentage:":int,
                   "Related Outcomes:":str -> list,
                   "Count Question?:":int
                }, ...
             }
           }, ...
       }
       '''
    data = pd.read_excel(file_path, sheet_name=2, skiprows=1, header=None, nrows=6)
    df = pd.DataFrame(data).set_index(3)
    current_exam = ''
    for i in range(5, df.shape[1] + 1):
        col = df[i][:].tolist()
        if not isinstance(col[0], float):
            current_exam = col[0]
            exams[current_exam]['Exam Percentage'] = col[1]
        exams[current_exam]['Questions'][col[2]] = {'Question Percentage': col[3],
                                                    'Related Outcomes': col[4],
                                                    'Count Question?': col[5]}


def gat_analyzer(file_path, section, department, code, year_and_term, name, credit):
    clear_global_variables()
    gat_conversion_sheet(file_path)
    gat_po_sheet(file_path)
    gat_student_info_sheet(file_path)
    gat_co_sheet(file_path)
    gat_evaluation_co_sheet(file_path)
    gat_grade_center_sheet(file_path)
    return start_threads(section, department, code, year_and_term, name, credit)

def gat_conversion_sheet(file_path):
    global conversion_dict
    data = pd.read_excel(file_path, sheet_name=0, skiprows=1)
    df = pd.DataFrame(data, columns=['100-Value', '5-Value'])
    conversion_dict = df.set_index('100-Value')['5-Value'].to_dict()


def gat_po_sheet(file_path):
    global program_outcomes
    data = pd.read_excel(file_path, sheet_name=1, skiprows=1)
    df = pd.DataFrame(data, columns=['Program Outcomes', 'Program Outcome Explanation'])
    program_outcomes = df.set_index('Program Outcomes')['Program Outcome Explanation'].to_dict()


def gat_student_info_sheet(file_path):
    global student_list
    data = pd.read_excel(file_path, sheet_name=2, skiprows=1)
    df = pd.DataFrame(data, columns=['Student ID', 'Student Name'])
    temp = list(df.set_index('Student ID')['Student Name'].to_dict().keys())
    for s in range(len(temp)):
        try:
            student_list.append(str(int(temp[s])))
        except:
            pass


def gat_co_sheet(file_path):
    global course_outcomes
    data = pd.read_excel(file_path, sheet_name=3, skiprows=1, index_col='Course Outcomes')
    df = pd.DataFrame(data, columns=['Course Outcome Explanation', 'Program Outcomes'])
    course_outcomes = df.to_dict()


def gat_evaluation_co_sheet(file_path):
    global exams
    data = pd.read_excel(file_path, sheet_name=4, skiprows=1)
    df = pd.DataFrame(data)
    temp = df.values.tolist()
    current_exam = ''
    for row in temp:
        if not isinstance(row[0], float):
            current_exam = row[0]
            exams[current_exam]['Exam Percentage'] = row[1]
        related_outcomes = []
        for i in range(4, len(row)):
            if not isinstance(row[i], float):
                related_outcomes.append(row[i])
        exams[current_exam]['Questions'][row[2]] = {'Question Percentage': row[3],
                                                    'Related Outcomes': str(', '.join(related_outcomes))}


def gat_grade_center_sheet(file_path):
    global exams, grading_tool_grades
    data = pd.read_excel(file_path, sheet_name=6)
    df = pd.DataFrame(data)

    col_count = 4
    for i in exams.keys():
        for j in exams[i]['Questions'].keys():
            col_count += 1
    for i in range(4, col_count):
        grading_tool_grades.append(list())
    df.drop(['Unnamed: 2', 'Unnamed: 3'], axis=1, inplace=True)
    for i in range(col_count, df.shape[1]):
        df.drop(df.columns[col_count], axis=1, inplace=True)
    df.dropna(subset=[df.columns[0]], inplace=True)
    temp = df.values.tolist()
    for count in temp[0][4:]:
        for i in exams.keys():
            for j in exams[i]['Questions'].keys():
                exams[i]['Questions'][j]['Count Question?'] = count
    for i in temp[4:]:
        for j in range(len(i[4:])):
            grading_tool_grades[j].append(i[j + 4])


file_path = "D://GitHub//SPA//spa-analyzer-flask//uploads//input.xlsx"
section = 7
department = 1
code = "MUH1396"
year_and_term= "2019-2020-01"
name = "Muhammed Test"
credit = 6


analyze_spa(file_path, section, department, code, year_and_term, name, credit)







'''
spa_course_sheet('/Users/alpgokcek/deneme/input.xlsx')
spa_program_sheet('/Users/alpgokcek/deneme/input.xlsx')
spa_grades_sheet('/Users/alpgokcek/deneme/input.xlsx')
student_answers_grading_tool()
'''
