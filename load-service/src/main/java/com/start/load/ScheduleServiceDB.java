package com.start.load;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.math.BigDecimal;
import java.sql.*;

/**
 * @author Roger Manzo
 * @apiNote Service Map
 */
@Configuration
public class ScheduleServiceDB {

    @Value("${spring.datasource.url}")
    private String databaseInt;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    @Value("${spring.datasource.url2}")
    private String databaseExtern;
    @Value("${spring.datasource.username2}")
    private String username2;
    @Value("${spring.datasource.password2}")
    private String password2;
    @Value("${spring.data.schedule.postgre}")
    private String postgre;
    @Value("${spring.data.schedule.truncate}")
    private String truncate;

    public static boolean isNull(BigDecimal bdg) {
        if(bdg == null){
            return bdg == null;
        }
        return false;
    }

    /* public void isRead(){
        FileReader reader;
        try {
            reader = new FileReader("C:/Users/Roger/Documents/slatool-api/load-service/src/main/resources/application.properties");
            Properties prop = new Properties();
            try {
                prop.load(reader);

                String cut = prop.getProperty("spring.data.schedule.truncate");
                int llaveInicio = cut.indexOf("{");
                int llaveUltima = cut.indexOf(":");
                String subCadena = cut.substring(llaveInicio+1, llaveUltima);
                System.out.println("${"+subCadena+"}");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }
     */

    @Scheduled(cron = "${spring.data.schedule.truncate}" , zone = "Europe/London")
    public void truncatedb(){
        Statement stmt;
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(databaseInt, username, password);
            if (conn != null) {
                System.out.println("Connection established");
            } else {
                System.out.println("Connection failed");
            }

            String querycommand = String.format("Delete from public.sla_command_staging where datetime < now() - interval '30 days'");

            String querymeter = String.format("Delete from public.sla_meter_staging where process_date < now() - interval '30 days'");

            stmt=conn.createStatement();
            stmt.executeUpdate(querycommand);
            stmt.executeUpdate(querymeter);
            System.out.println("Truncate sucess...");

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Scheduled(cron = "${spring.data.schedule.postgre}", zone = "Europe/London")
    public void readWrite(){
        Statement stmt;
        String table_name = "SLA_Meter_Staging";
        try {
            Class.forName("org.postgresql.Driver"); //jdbc:postgres://host:port/database
            Connection conn= DriverManager.getConnection(databaseInt, username, password);
            if(conn != null){
                System.out.println("Connection established");
            } else {
                System.out.println("Connection failed");
            }
            String query= "";

            try {
                Connection conn2 = DriverManager.getConnection(databaseExtern, username2, password2);
                if(conn2 != null){
                    System.out.println("Connection extern sucess");
                } else {
                    System.out.println("Connection failed");
                }
                //query read aadc organization

                query = String.format("select mp.priority, " +
                        "e.serial_number, " +
                        "(select mpd.city " +
                        "from aadc.measuring_point_location_data mpd " +
                        "where mpd.id = mp.location_data) as zone, current_date - 1 as process_date, " +
                        "'AADC' as organization, eqs.code as status " +
                        "from aadc.equipment_point_configuration epc, " +
                        "aadc.equipment e, " +
                        "aadc.measuring_point mp, aadc.equipment_status eqs, " +
                        "aadc.historical_equipment_status heqs " +
                        "where e.id = epc.equipment " +
                        "and epc.measuring_point = mp.id " +
                        "and e.id = heqs.equipment " +
                        "and heqs.status = eqs.id " +
                        "and heqs.register_date = (select MAX(heqs1.register_date) " +
                        "from aadc.historical_equipment_status heqs1 " +
                        "where e.id = heqs1.equipment)");


                //execute read data extern and push my db
                stmt=conn2.createStatement();
                ResultSet rs=stmt.executeQuery(query);
                System.out.println("Data write...");
                Integer a = 0;
                while(rs.next()){
                    System.out.println("Read process:"+a);
                    ++a;
                    BigDecimal pbool = rs.getBigDecimal("priority");

                    if(isNull(pbool)){
                        String priority = "P3";
                        String serialNumber = rs.getString("serial_number");
                        String zone = rs.getString("zone");
                        String organization = rs.getString("organization");
                        String status = rs.getString("status");
                        try {
                            Date date = new Date(rs.getDate("process_date").getTime());
                            //System.out.println(date);
                            stmt = conn.createStatement();
                            String qr = String.format("insert into "+table_name+" (Priority, Serial_Number, Zone, Process_date, Organization, Status) " +
                                            "values ('%s', '%s', '%s', '%s', '%s', '%s')",
                                    priority, serialNumber, zone, date, organization, status);
                            stmt.executeUpdate(qr);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    } else {
                        String priority = rs.getString("priority");
                        String px = "P"+priority;
                        //System.out.println("PX="+px);
                        String serialNumber = rs.getString("serial_number");
                        String zone = rs.getString("zone");
                        //System.out.println("serialNumber="+serialNumber);
                        String organization = rs.getString("organization");
                        String status = rs.getString("status");
                        try {
                            Date date = new Date(rs.getDate("process_date").getTime());
                            //System.out.println(date);
                            stmt = conn.createStatement();
                            String qr = String.format("insert into "+table_name+" (Priority, Serial_Number, Zone, Process_date, Organization, Status) " +
                                            "values ('%s', '%s', '%s', '%s', '%s', '%s')",
                                    px, serialNumber, zone, date, organization, status);
                            stmt.executeUpdate(qr);
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                }

                System.out.println("Sucess extract data [aadc] in database extern");

                try {
                    conn2 = DriverManager.getConnection(databaseExtern, username, password);
                    if(conn2 != null){
                        System.out.println("Connection extern sucess");
                    } else {
                        System.out.println("Connection failed");
                    }
                    //query read addc organization

                    query = String.format("select mp.priority, " +
                            "e.serial_number, " +
                            "(select mpd.city " +
                            "from addc.measuring_point_location_data mpd " +
                            "where mpd.id = mp.location_data) as zone, " +
                            "current_date - 1 as process_date, " +
                            "'ADDC' as organization, eqs.code as status " +
                            "from addc.equipment_point_configuration epc, " +
                            "addc.equipment e, " +
                            "addc.measuring_point mp, addc.equipment_status eqs, " +
                            "addc.historical_equipment_status heqs " +
                            "where e.id = epc.equipment " +
                            "and epc.measuring_point = mp.id " +
                            "and e.id = heqs.equipment " +
                            "and heqs.status = eqs.id " +
                            "and heqs.register_date = (select MAX(heqs1.register_date) " +
                            "from addc.historical_equipment_status heqs1 " +
                            "where e.id = heqs1.equipment)");

                    //execute read data extern and push my db
                    stmt=conn2.createStatement();
                    rs=stmt.executeQuery(query);
                    System.out.println("Data write...");
                    while(rs.next()) {
                        System.out.println("Read process:"+a);
                        ++a;
                        BigDecimal pbool = rs.getBigDecimal("priority");

                        if(isNull(pbool)){
                            String priority = "P3";
                            //System.out.println("Priority=" + priority);
                            String serialNumber = rs.getString("serial_number");
                            String zone = rs.getString("zone");
                            //System.out.println("serialNumber="+serialNumber);
                            String organization = rs.getString("organization");
                            String status = rs.getString("status");
                            try {
                                Date date = new Date(rs.getDate("process_date").getTime());
                                //System.out.println(date);
                                stmt = conn.createStatement();
                                String qr = String.format("insert into "+table_name+" (Priority, Serial_Number, Zone, Process_date, Organization, Status) " +
                                                "values ('%s', '%s', '%s', '%s', '%s', '%s')",
                                        priority, serialNumber, zone, date, organization, status);
                                stmt.executeUpdate(qr);
                                System.out.println(qr);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        } else {
                            String priority = rs.getString("priority");
                            String px = "P"+priority;
                            //System.out.println("PX="+px);
                            String serialNumber = rs.getString("serial_number");
                            String zone = rs.getString("zone");
                            //System.out.println("serialNumber="+serialNumber);
                            String organization = rs.getString("organization");
                            String status = rs.getString("status");
                            try {
                                Date date = new Date(rs.getDate("process_date").getTime());
                                //System.out.println(date);
                                stmt = conn.createStatement();
                                String qr = String.format("insert into "+table_name+" (Priority, Serial_Number, Zone, Process_date, Organization, Status) " +
                                                "values ('%s', '%s', '%s', '%s', '%s', '%s')",
                                        px, serialNumber, zone, date, organization, status);
                                stmt.executeUpdate(qr);
                                System.out.println(qr);
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                    System.out.println("Sucess extract data [addc] in database extern" +
                    "\nFinish the process load-service");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e ) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
