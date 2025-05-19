# Apache Kafka Temperature & Humidity Sensor

Mata Kuliah: Big Data dan Data Lakehouse (A)
Dosen Pengampu : Fuad Dary Rosyadi, S.Kom., M.Kom.

| Nama                     | NRP        |
| ------------------------ | ---------- |
| Rafael Gunawan | 5027231019 |

## Langkah Pengerjaan
1. Install Kafka + Spark. <br> <br> <br>
2. Jalankan server Kafka di <strong>Terminal 1<strong>. <br>
   `bin/kafka-server-start.sh config/server.properties` <br> <br>
   ![image](https://github.com/user-attachments/assets/d7ce4ac6-59d6-4d1d-81d3-40b95902b014)
   <br>

3. Buat topik Kafka untuk masing-masing producer (Suhu dan Kelembaban Gudang) di <strong>Terminal 2<strong>. <br>
   `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic sensor-suhu-gudang --partitions 1 --replication-factor 1` <br>
   `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic sensor-kelembaban-gudang --partitions 1 --replication-factor 1` <br> <br>
   ![image](https://github.com/user-attachments/assets/c8a3ee4a-927d-423c-a484-c43031ed2d40)
   #### Topik disini berguna untuk mengirim dan menerima data sensor suhu dan kelembaban dari gudang.
   <br> <br> <br>

4. Jalankan producer suhu di <strong>Terminal 2<strong>. <br>
   `python temperature_producer.py` <br> <br>
   ![image](https://github.com/user-attachments/assets/bc2d86f0-baf3-4867-ab55-65a505fd7ed1)
   #### Producer akan mengirim data JSON dari 3 gudang yang memiliki suhu dari rentang 70-90 setiap 1 detik.
   <br> <br> <br>

5. Jalankan producer kelembaban di <strong>Terminal 3<strong>. <br>
   `python humidity_producer.py` <br> <br>
   ![image](https://github.com/user-attachments/assets/c601d595-0529-47b7-a7cd-e6ddee39c4ff)
   #### Producer akan mengirim data JSON dari 3 gudang yang memiliki kelembaban dari rentang 60-80 setiap 1 detik.
   <br> <br> <br>

6. Jalankan Spark streaming (consumer) di <strong>Terminal 4<strong>. <br>
   `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --driver-memory 2g --conf "spark.driver.extraJavaOptions=-Djava.net.preferIPv4Stack=true" spark_consumer.py` <br> <br>
   ![image](https://github.com/user-attachments/assets/cfcd0275-96fd-43ca-b561-1d07c52f8c09)
   #### Consumer akan membaca dan memfilter data suhu dan kelembaban dari prodecure kafka tadi, kemudian akan menampilkan hasilnya secara real-time.
