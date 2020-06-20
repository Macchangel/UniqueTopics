CREATE TABLE IF NOT EXISTS news_test3(
    id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    news_title VARCHAR(100) NOT NULL,
    news_url VARCHAR(100) NOT NULL,
    news_date DATE,
    news_location VARCHAR(10),
    news_child_location VARCHAR(10),
    news_text TEXT,
    news_words TEXT
    )ENGINE=InnoDB DEFAULT CHARSET=utf8;


ALTER TABLE news_test ADD COLUMN news_words TEXT;

