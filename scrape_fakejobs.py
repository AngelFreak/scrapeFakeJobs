import requests, sqlite3
from bs4 import BeautifulSoup
from sqlite3 import Error

def createConnection(dbFile):
    conn = None
    try:
        conn = sqlite3.connect(dbFile)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close
    return conn

def createTable(conn, createTableSql):
    """ create a table from the createTableSql statement
    :param conn: Connection object
    :param createTableSql: a CREATE TABLE statement
    :return:
    """
    try:
        cursor = conn.cursor()
        cursor.execute(createTableSql)
    except Error as e:
        print(e)

def createJob(conn, job):
    """
    Create a new job entry into the python_jobs table
    :param conn:
    :param job:
    """
    sql = ''' INSERT INTO python_jobs(title,company,location,description,posted,apply_here)
              VALUES(?,?,?,?,?,?) '''
    cursor = conn.cursor()
    cursor.execute(sql, job)
    conn.commit()

def scrapeFakeJobs(conn, url):
    page = requests.get(url)

    soup = BeautifulSoup(page.content, "html.parser")

    results = soup.find(id="ResultsContainer")
    jobElements = results.find_all("div", class_="card-content")

    pythonJobs = results.find_all(
        "h2", string=lambda text: "python" in text.lower()
    )
    pythonJobElements = [
        h2_element.parent.parent.parent for h2_element in pythonJobs
    ]

    for jobElement in pythonJobElements:
        titleElement = jobElement.find("h2", class_="title")
        companyElement = jobElement.find("h3", class_="company")
        locationElement = jobElement.find("p", class_="location")
        datePostedElement = jobElement.find("time")
        linkUrl = jobElement.find_all("a")[1]["href"]

        jobPage = requests.get(linkUrl)
        jobSoup = BeautifulSoup(jobPage.content, "html.parser")
        jobResult = jobSoup.find("div", class_="content")

        jobDescription = jobResult.find("p")

        with conn:
            jobData = (titleElement.text.strip(), companyElement.text.strip(), locationElement.text.strip(), jobDescription.text.strip(), datePostedElement.text.strip(), linkUrl)
            createJob(conn, jobData)
    #    print('Title: ' + titleElement.text.strip())
    #    print('Company: ' + companyElement.text.strip())
    #    print('Location: ' + locationElement.text.strip())
    #    print('Description: ' + jobDescription.text.strip())
    #    print('Posted: ' + datePostedElement.text.strip())
    #    print(f'Apply here: {linkUrl}\n')
    #    print()
    #print('Total number of pyhton jobs: ' + str(len(pythonJobElements)))

def main():
    database = r"jobdb.db"

    sql_create_python_jobs_table = """ CREATE TABLE IF NOT EXISTS python_jobs (
                                        id integer PRIMARY KEY,
                                        title text NOT NULL,
                                        company text,
                                        location text,
                                        description text,
                                        posted text,
                                        apply_here text,
                                        UNIQUE (title, company, location, description) ON CONFLICT IGNORE
                                    ); """

    # create a database connection
    conn = createConnection(database)

    # create tables
    if conn is not None:
        # create python_jobs table
        createTable(conn, sql_create_python_jobs_table)
    else:
        print("Error! cannot create the database connection.")

    # scrape the following sites
    scrapeFakeJobs(conn, 'https://realpython.github.io/fake-jobs/')

if __name__ == '__main__':
    main()