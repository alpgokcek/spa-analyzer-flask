import pandas as pd
from collections import defaultdict
import json
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import datetime
from dotenv import load_dotenv
from os import environ

load_dotenv()


def get_result_proxy(resultproxy):
    for rowproxy in resultproxy:
        # rowproxy.items() returns an array like [(key0, value0), (key1, value1)]
        for column, value in rowproxy.items():
            return value


def nested_defaultdict():
    return defaultdict(nested_defaultdict)


program_outcomes, course_outcomes, student_list, exams, grading_tool_grades = dict(), dict(), list(), defaultdict(
    nested_defaultdict), list()
course_code, course_name, course_credit = '', '', '',
program_outcomes_id, course_outcomes_id, course_id, student_id, exam_id, grading_tool_id = dict(), dict(), 0, dict(), dict(), defaultdict(
    list)

DB_USER, DB_PASS, DB_HOST, DB_PORT, DATABASE = environ.get('DB_USER'), environ.get('DB_PASS'), environ.get(
    'DB_HOST'), environ.get('DB_PORT'), environ.get('DATABASE')

# create sqlalchemy engine
connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DATABASE)

engine = create_engine(connect_string, poolclass=NullPool)


def analyze_spa(file_path):
    program_sheet(file_path)
    course_sheet(file_path)
    grades_sheet(file_path)

    ###################################################################################################
    ########################################## COURSE #################################################
    ###################################################################################################

    print("Course")
    conn = engine.connect()
    metadata = sqlalchemy.MetaData()
    course_table = sqlalchemy.Table('course', metadata, autoload=True, autoload_with=engine)
    course_query = sqlalchemy.select([course_table]).where(
        sqlalchemy.and_(course_table.columns.department == 1, course_table.columns.code == course_code,
                        course_table.columns.year_and_term == '2019-2020-01', course_table.columns.title == course_name,
                        course_table.columns.credit == course_credit))
    course_id = get_result_proxy(conn.execute(course_query))

    ###################################################################################################
    ##################################### PROGRAM OUTCOMES ############################################
    ###################################################################################################
    print("Program Outcomes")
    for i in program_outcomes.keys():
        now = datetime.datetime.utcnow()
        program_outcome = pd.DataFrame(
            {'department_id': 1, 'explanation': program_outcomes[i], 'code': i, 'created_at': now, 'updated_at': now},
            index=[0])
        program_outcome.to_sql('program_outcome', con=conn, if_exists='append', chunksize=1000, index=False)
        last_id = conn.execute("SELECT LAST_INSERT_ID();")
        program_outcomes_id[i] = get_result_proxy(last_id)

    ###################################################################################################
    ###################################### COURSE OUTCOMES ############################################
    ###################################################################################################
    print("Course Outcomes")
    for i in course_outcomes['Course Outcome Explanation'].keys():
        now = datetime.datetime.utcnow()
        course_outcome = pd.DataFrame(
            {'course_id': course_id, 'explanation': course_outcomes['Course Outcome Explanation'][i], 'code': i,
             'created_at': now, 'updated_at': now},
            index=[0])
        course_outcome.to_sql('course_outcome', con=conn, if_exists='append', chunksize=1000, index=False)
        last_id = conn.execute("SELECT LAST_INSERT_ID();")
        course_outcomes_id[i] = get_result_proxy(last_id)

    ###################################################################################################
    ############################### COURSE OUTCOMES - PROGRAM OUTCOMES ################################
    ###################################################################################################
    print("program outcomes provides course outcomes")
    for i in course_outcomes['Program Outcomes'].keys():
        now = datetime.datetime.utcnow()
        splitted_program_outcomes = [x.strip() for x in course_outcomes['Program Outcomes'][i].split(',')]
        for j in splitted_program_outcomes:
            program_outcomes_provides_course_outcomes = pd.DataFrame(
                {'course_outcome_id': course_outcomes_id[i], 'program_outcome_id': program_outcomes_id[j],
                 'created_at': now, 'updated_at': now},
                index=[0])
            program_outcomes_provides_course_outcomes.to_sql('program_outcomes_provides_course_outcomes', con=conn,
                                                             if_exists='append', chunksize=1000, index=False)

    ###################################################################################################
    ################################## GRADING TOOL and ASSESSMENTS ###################################
    ###################################################################################################
    print("grading tool and assessments")
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

    ###################################################################################################
    ############################### GRADING TOOL COVERS COURSE OUTCOME ################################
    ###################################################################################################
    print("grading tool covers course outcome")
    for exam in exams.keys():
        now = datetime.datetime.utcnow()
        for i in range(len(exams[exam]['Questions'].keys())):
            question = list(exams[exam]['Questions'].keys())[i]
            splitted_course_outcomes = [x.strip() for x in
                                        exams[exam]['Questions'][question]['Related Outcomes'].split(',')]
            for co in splitted_course_outcomes:
                course_outcome = pd.DataFrame(
                    {'grading_tool_id': grading_tool_id[exam][i], 'course_outcome_id': course_outcomes_id[co],
                     'created_at': now, 'updated_at': now}, index=[0])
                course_outcome.to_sql('grading_tool_covers_course_outcome', con=conn, if_exists='append',
                                      chunksize=1000,
                                      index=False)

    ###################################################################################################
    ################################## STUDENT ANSWERS GRADING TOOL ###################################
    ###################################################################################################
    print("student answers grading tool")
    j = 0
    for exam in exams.keys():
        for gt_id in grading_tool_id[exam]:
            for i in range(len(student_list)):
                now = datetime.datetime.utcnow()
                sagt = pd.DataFrame(
                    {'student_id': student_list[i], 'grading_tool_id': gt_id, 'grade': grading_tool_grades[j][i],
                     'created_at': now,
                     'updated_at': now},
                    index=[0])
                sagt.to_sql('student_answers_grading_tool', con=conn, if_exists='append', chunksize=1000, index=False)
            j += 1
    conn.close()
    engine.dispose()


def program_sheet(file_path):
    global program_outcomes
    data = pd.read_excel(file_path, sheet_name=0, skiprows=1)
    df = pd.DataFrame(data, columns=['Program Outcomes', 'Program Outcome Explanation'])
    program_outcomes = df.set_index('Program Outcomes')['Program Outcome Explanation'].to_dict()


def course_sheet(file_path):
    global course_outcomes, course_code, course_name, course_credit
    data = pd.read_excel(file_path, sheet_name=1, skiprows=5, index_col='Course Outcomes')
    df = pd.DataFrame(data, columns=['Course Outcome Explanation', 'Program Outcomes'])
    course_outcomes = df.to_dict()
    data = pd.read_excel(file_path, sheet_name=1, header=None)
    df = pd.DataFrame(data)
    course_code, course_name, course_credit = df.iat[0, 1], df.iat[1, 1], df.iat[2, 1]


def grades_sheet(file_path):
    global course_outcomes, course_code, course_name, course_credit, exams, student_list
    data = pd.read_excel(file_path, sheet_name=2, skiprows=9, header=None)
    df = pd.DataFrame(data)

    # student id's
    temp_student_list = data.iloc[0:, 0].ravel().tolist()
    conn = engine.connect()
    print("checking student list")
    for i in temp_student_list:
        metadata = sqlalchemy.MetaData()
        students_takes_sections_table = sqlalchemy.Table('students_takes_sections', metadata, autoload=True,
                                                         autoload_with=engine)
        students_takes_sections_query = sqlalchemy.select([students_takes_sections_table]).where(
            sqlalchemy.and_(students_takes_sections_table.columns.student_id == i,
                            students_takes_sections_table.columns.section_id == 1))
        students_takes_sections = get_result_proxy(conn.execute(students_takes_sections_query))
        if students_takes_sections:
            student_list.append(i)
    conn.close()

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
        exams[current_exam]['Questions'][col[2]] = {'Question Percentage': col[3], 'Related Outcomes': col[4],
                                                    'Count Question?': col[5]}
