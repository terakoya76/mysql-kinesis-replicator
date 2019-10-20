setup: setup.kinesis setup.mysql

setup.kinesis:
	aws kinesis create-stream --stream-name test-stream --shard-count 1 --endpoint-url http://localhost:4568 --no-verify-ssl

setup.mysql:
	mysql -u root -h 127.0.0.1 -e "CREATE DATABASE IF NOT EXISTS mysql_kinesis_streamer_db;"
	mysql -uroot -h 127.0.0.1 -e "CREATE TABLE IF NOT EXISTS mysql_kinesis_streamer_db.test (id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, foo VARCHAR(255) NOT NULL, bar TEXT);"
	mysql -uroot -h 127.0.0.1 -e "INSERT INTO repl_example.test (foo, bar) VALUES ('foo1', 'bar1'), ('foo2', 'bar2'), ('foo3', 'bar3');"
