package schedule;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class OpenDart_Daily implements Runnable{

    @Override
    public void run() {
        try {
            LocalDateTime now = LocalDateTime.now();
            System.out.println("=====" + now + " 오픈다트 데이터 수집 실행 =====");

            // 기업고유번호 조회
            JsonArray newCorpCodeJsonArr = retrieveCorpCode();
            // 기업고유번호 DB MERGE
            insertCorpCode(newCorpCodeJsonArr);
            // 기업개황 조회
            JsonArray newCompanyJsonArr = retrieveCompany(newCorpCodeJsonArr);
            // 기업개황 DB MERGE
            insertCompany(newCompanyJsonArr);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 기업고유번호 조회
    public static JsonArray retrieveCorpCode() throws ClassNotFoundException {

        String crtfc_key = "a1ab4687628095bbae0fd90f4c34c9c897fda441";
        String zipFileNams = "corpCode.zip";
        String xmlFileName = "corpCode.xml";

        JsonArray corpCodeJsonArr = new JsonArray();

        try {
            System.out.println("===== 기업고유번호 API 호출 =====");
            // 1. API URL 생성
            String encodedKey = URLEncoder.encode(crtfc_key, "UTF-8");
            String apiUrl = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key=" + encodedKey;

            HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // 2. ZIP 파일 다운로드
                try (BufferedInputStream bis = new BufferedInputStream(conn.getInputStream());
                     FileOutputStream fos = new FileOutputStream(zipFileNams)) {

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = bis.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                    }

                    System.out.println("전체 기업코드 XML 파일 다운로드 완료 : " + zipFileNams);
                }
            } else {
                System.out.println("HTTP 요청 실패, 코드 : " + responseCode);
            }

            conn.disconnect();

            // 3. 압축 해제 (현재 디렉토리)
            try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFileNams))) {
                ZipEntry entry;
                while ((entry = zis.getNextEntry()) != null) {
                    String outPath = entry.getName(); // ZIP 안 파일 이름 그대로 사용

                    try (FileOutputStream fos = new FileOutputStream(outPath)) {
                        byte[] buffer = new byte[4096];
                        int bytesRead;
                        while ((bytesRead = zis.read(buffer)) != -1) {
                            fos.write(buffer, 0, bytesRead);
                        }
                    }
                    zis.closeEntry();
                    System.out.println("압축 해제 완료 : " + outPath);
                }
            }

            // 4. zip 파일 삭제
            File zipFile = new File(zipFileNams);
            if (zipFile.exists() && zipFile.delete()) {
                System.out.println("ZIP 파일 삭제 완료 : " + zipFileNams);
            } else {
                System.out.println("ZIP 파일 삭제 실패 : " + zipFileNams);
            }

            // 5. xml 파일 파싱, json 파일 생성
            File xmlFile = new File(xmlFileName);
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList list = doc.getElementsByTagName("list");

            for (int i = 0; i < list.getLength(); i++) {
                Element ele = (Element) list.item(i);

                JsonObject obj = new JsonObject();

                // 마지막에 큰따옴표 있을 시 삭제 -> csv 변환 시 컬럼 경계 깨지는 현상 발생
                String corp_code = ele.getElementsByTagName("corp_code").item(0).getTextContent().trim();
                if (corp_code.endsWith("\"")) corp_code = corp_code.substring(0, corp_code.length() - 1);
                String corp_name = ele.getElementsByTagName("corp_name").item(0).getTextContent().trim();
                if (corp_name.endsWith("\"")) corp_name = corp_name.substring(0, corp_name.length() - 1);
                String corp_eng_name = ele.getElementsByTagName("corp_eng_name").item(0).getTextContent().trim();
                if (corp_eng_name.endsWith("\""))
                    corp_eng_name = corp_eng_name.substring(0, corp_eng_name.length() - 1);
                String stock_code = ele.getElementsByTagName("stock_code").item(0).getTextContent().trim();
                if (stock_code.endsWith("\"")) stock_code = stock_code.substring(0, stock_code.length() - 1);
                String modify_date = ele.getElementsByTagName("modify_date").item(0).getTextContent().trim();
                if (modify_date.endsWith("\"")) modify_date = modify_date.substring(0, modify_date.length() - 1);

                obj.addProperty("corp_code", corp_code);
                obj.addProperty("corp_name", corp_name);
                obj.addProperty("corp_eng_name", corp_eng_name);
                obj.addProperty("stock_code", stock_code);
                obj.addProperty("modify_date", modify_date);

                corpCodeJsonArr.add(obj);
            }

            // 6. xml 파일 삭제
            if (xmlFile.exists() && xmlFile.delete()) {
                System.out.println("XML 파일 삭제 완료 : " + xmlFileName);
            } else {
                System.out.println("XML 파일 삭제 실패 : " + xmlFileName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        // 새로운 기업고유번호 조회
        Set<String> oldCorpCodeSet = new HashSet<>();
        Set<String> newCorpCodeSet = new HashSet<>();

        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection connection = DriverManager.getConnection(url, user, password);) {
            Statement statement = connection.createStatement();
            /* SELECT */
            ResultSet rs = statement.executeQuery("SELECT * FROM CORP_CODE WHERE 1=1");

            while (rs.next()) {
                oldCorpCodeSet.add(rs.getString("CORP_CODE"));
            }
            rs.close();
            statement.close();
            connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (JsonElement el : corpCodeJsonArr) {
            JsonObject corpObj = el.getAsJsonObject();
            if (corpObj.has("corp_code")) {
                newCorpCodeSet.add(corpObj.get("corp_code").getAsString());
            }
        }

        Set<String> resultCorpCodeSet = new HashSet<>(newCorpCodeSet);
        resultCorpCodeSet.removeAll(oldCorpCodeSet);

        Calendar cal = Calendar.getInstance();
        String format = "yyyyMMdd";
        SimpleDateFormat sdf1 = new SimpleDateFormat(format);
        cal.add(cal.DATE, -1);
        String date = sdf1.format(cal.getTime());

        for (JsonElement ele : corpCodeJsonArr) {
            JsonObject corpCodeObj = ele.getAsJsonObject();
            String modifyDate = corpCodeObj.get("modify_date").getAsString();

            if (modifyDate.compareTo(date) >= 0) {
                resultCorpCodeSet.add(corpCodeObj.get("corp_code").getAsString());
            }
        }

        System.out.println("업데이트 된 기업고유번호 " + resultCorpCodeSet.size() + "건");
        resultCorpCodeSet.forEach(corpCode -> System.out.println("업데이트 된 기업고유번호 : " + corpCode));

        JsonArray newCorpCodeJsonArr = new JsonArray();

        for (JsonElement ele : corpCodeJsonArr) {
            JsonObject corpCodeObj = ele.getAsJsonObject();
            String corpCode = corpCodeObj.get("corp_code").getAsString();

            if (resultCorpCodeSet.contains(corpCode)) {
                newCorpCodeJsonArr.add(corpCodeObj);
            }
        }
        return newCorpCodeJsonArr;
    }


    public static void insertCorpCode(JsonArray newCorpCodeJsonArr) throws ClassNotFoundException {
        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection connection = DriverManager.getConnection(url, user, password);) {
            Statement statement = connection.createStatement();

            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now = sdf2.format(new Date());

            String sql = "MERGE INTO CORP_CODE AS A " +
                         "USING (VALUES (?, ?, ?, ?, ?)) AS B (CORP_CODE, CORP_ENG_NAME, CORP_NAME, STOCK_CODE, MODIFY_DATE)" +
                         "ON A.CORP_CODE = B.CORP_CODE " +
                         "WHEN MATCHED THEN " +
                             "UPDATE SET CORP_CODE = B.CORP_CODE, " +
                                        "CORP_ENG_NAME = B.CORP_ENG_NAME, " +
                                        "CORP_NAME = B.CORP_NAME, " +
                                        "STOCK_CODE = B.STOCK_CODE, " +
                                        "MODIFY_DATE = B.MODIFY_DATE, " +
                                        "LAST_CHNG_DTL_DTTM = '" + now + "' " +
                         "WHEN NOT MATCHED THEN " +
                             "INSERT (CORP_CODE, CORP_ENG_NAME, CORP_NAME, STOCK_CODE, MODIFY_DATE, DEL_YN, FRST_RGSR_DTL_DTTM, LAST_CHNG_DTL_DTTM) " +
                             "VALUES (B.CORP_CODE, B.CORP_ENG_NAME, B.CORP_NAME, B.STOCK_CODE, B.MODIFY_DATE, 'N', '" + now +  "', '" + now + "')";

            PreparedStatement ps = connection.prepareStatement(sql);

            System.out.println("===== 기업고유번호 (CORP_CODE) DB MERGE =====");

            for (JsonElement el : newCorpCodeJsonArr) {
                String corpCode = el.getAsJsonObject().get("corp_code").getAsString();
                String corpEngName = el.getAsJsonObject().get("corp_eng_name").getAsString();
                String corpName = el.getAsJsonObject().get("corp_name").getAsString();
                String stockCode = el.getAsJsonObject().get("stock_code").getAsString();
                String modifyDate = el.getAsJsonObject().get("modify_date").getAsString();

                ps.setString(1, corpCode);
                ps.setString(2, corpEngName);
                ps.setString(3, corpName);
                ps.setString(4, stockCode);
                ps.setString(5, modifyDate);

                int result = ps.executeUpdate();

                if (result > 0) {
                    System.out.println("[SUCCESS] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> MERGE 처리");
                } else {
                    System.out.println("[NO CHANGE] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> 변경 없음");
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static JsonArray retrieveCompany(JsonArray newCorpCodeJsonArr) {
        // 새로운 기업고유번호에 대한 기업개황 데이터 조회
        JsonArray newCompanyJsonArr = new JsonArray();

        int total = newCorpCodeJsonArr.size();
        int cnt = 1;

        String crtfc_key = "a1ab4687628095bbae0fd90f4c34c9c897fda441";
        // 오픈 다트 하루 수집량 10,000건으로 초기 데이터 적재 시 수집한 기업고유번호 데이터 갯수에 따른 조정 필요 (corpCodeJsonArr)
        int batchSize = newCorpCodeJsonArr.size();
        // API 차단 방지용
        int delayMillis = 600;

        System.out.println("===== 기업개황 API 호출 =====");
        for (JsonElement ele : newCorpCodeJsonArr) {
            String corpCode = ele.getAsJsonObject().get("corp_code").getAsString();
            try {
                String encodedKey = URLEncoder.encode(crtfc_key, "UTF-8");
                String apiUrl = "https://opendart.fss.or.kr/api/company.json?crtfc_key=" + encodedKey + "&corp_code=" + corpCode;
                URL url2 = new URL(apiUrl);
                HttpURLConnection conn = null;
                conn = (HttpURLConnection) url2.openConnection();
                conn.setRequestMethod("GET");

                int responseCode = conn.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    // 응답(InputStream) 읽기
                    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                    StringBuilder sb = new StringBuilder();
                    String line;

                    while ((line = rd.readLine()) != null) {
                        sb.append(line);
                    }
                    rd.close();
                    conn.disconnect();

                    JsonObject jsonObject = new JsonParser().parse(sb.toString()).getAsJsonObject();

                    newCompanyJsonArr.add(jsonObject);

                    System.out.println("기업 개황 데이터 수집 " + cnt + "/" + total);
                    cnt++;

                } else {
                    System.out.println("HTTP 요청 실패, 코드 : " + responseCode);
                    throw new IOException("HTTP 요청 실패, 코드 : " + responseCode);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return newCompanyJsonArr;
    }

    public static void insertCompany(JsonArray newCompanyJsonArr) throws ClassNotFoundException {

        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";

        try (Connection connection = DriverManager.getConnection(url, user, password);) {
            Statement statement = connection.createStatement();

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now = sdf.format(new Date());

            String sql = "MERGE INTO COMPANY AS A " +
                         "USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS B " +
                                        "(PHN_NO, ACC_MT, CEO_NM, STOCK_NAME, CORP_CODE, INDUTY_CODE, JURIR_NO, MESSAGE, CORP_NAME, EST_DT, HM_URL, CORP_CLS, CORP_NAME_ENG, IR_URL, ADRES, STOCK_CODE, BIZR_NO, FAX_NO, STATUS)" +
                         "ON A.CORP_CODE = B.CORP_CODE " +
                         "WHEN MATCHED THEN " +
                             "UPDATE SET PHN_NO = B.PHN_NO, " +
                                        "ACC_MT = B.ACC_MT, " +
                                        "CEO_NM = B.CEO_NM, " +
                                        "STOCK_NAME = B.STOCK_NAME, " +
                                        "CORP_CODE = B.CORP_CODE, " +
                                        "INDUTY_CODE = B.INDUTY_CODE, " +
                                        "JURIR_NO = B.JURIR_NO, " +
                                        "MESSAGE = B.MESSAGE, " +
                                        "CORP_NAME = B.CORP_NAME, " +
                                        "EST_DT = B.EST_DT, " +
                                        "HM_URL = B.HM_URL, " +
                                        "CORP_CLS = B.CORP_CLS, " +
                                        "CORP_NAME_ENG = B.CORP_NAME_ENG, " +
                                        "IR_URL = B.IR_URL, " +
                                        "ADRES = B.ADRES, " +
                                        "STOCK_CODE = B.STOCK_CODE, " +
                                        "BIZR_NO = B.BIZR_NO, " +
                                        "FAX_NO = B.FAX_NO, " +
                                        "STATUS = B.STATUS, " +
                                        "LAST_CHNG_DTL_DTTM = '" + now + "' " +
                         "WHEN NOT MATCHED THEN " +
                             "INSERT (PHN_NO, ACC_MT, CEO_NM, STOCK_NAME, CORP_CODE, INDUTY_CODE, JURIR_NO, MESSAGE, CORP_NAME, EST_DT, HM_URL, CORP_CLS, CORP_NAME_ENG, IR_URL, ADRES, STOCK_CODE, BIZR_NO, FAX_NO, STATUS, DEL_YN, FRST_RGSR_DTL_DTTM, LAST_CHNG_DTL_DTTM) " +
                             "VALUES (B.PHN_NO, B.ACC_MT, B.CEO_NM, B.STOCK_NAME, B.CORP_CODE, B.INDUTY_CODE, B.JURIR_NO, B.MESSAGE, B.CORP_NAME, B.EST_DT, B.HM_URL, B.CORP_CLS, B.CORP_NAME_ENG, B.IR_URL, B.ADRES, B.STOCK_CODE, B.BIZR_NO, B.FAX_NO, B.STATUS, 'N', '" + now + "', '" + now + "')";

            PreparedStatement ps = connection.prepareStatement(sql);

            System.out.println("=== 기업개황 (COMPANY) DB MERGE =====");

            for (JsonElement e : newCompanyJsonArr) {
                String phnNo = e.getAsJsonObject().get("phn_no").getAsString();
                String accMt = e.getAsJsonObject().get("acc_mt").getAsString();
                String ceoNm = e.getAsJsonObject().get("ceo_nm").getAsString();
                String stockName = e.getAsJsonObject().get("stock_name").getAsString();
                String corpCode = e.getAsJsonObject().get("corp_code").getAsString();
                String indutyCode = e.getAsJsonObject().get("induty_code").getAsString();
                String jurirNo = e.getAsJsonObject().get("jurir_no").getAsString();
                String message = e.getAsJsonObject().get("message").getAsString();
                String corpName = e.getAsJsonObject().get("corp_name").getAsString();
                String estDT = e.getAsJsonObject().get("est_dt").getAsString();
                String hmUrl = e.getAsJsonObject().get("hm_url").getAsString();
                String corpCLS = e.getAsJsonObject().get("corp_cls").getAsString();
                String corpNameEng = e.getAsJsonObject().get("corp_name_eng").getAsString();
                String irUrl = e.getAsJsonObject().get("ir_url").getAsString();
                String adres = e.getAsJsonObject().get("adres").getAsString();
                String stockCode = e.getAsJsonObject().get("stock_code").getAsString();
                String bizrNo = e.getAsJsonObject().get("bizr_no").getAsString();
                String faxNo = e.getAsJsonObject().get("fax_no").getAsString();
                String status = e.getAsJsonObject().get("status").getAsString();

                ps.setString(1, phnNo);
                ps.setString(2, accMt);
                ps.setString(3, ceoNm);
                ps.setString(4, stockName);
                ps.setString(5, corpCode);
                ps.setString(6, indutyCode);
                ps.setString(7, jurirNo);
                ps.setString(8, message);
                ps.setString(9, corpName);
                ps.setString(10, estDT);
                ps.setString(11, hmUrl);
                ps.setString(12, corpCLS);
                ps.setString(13, corpNameEng);
                ps.setString(14, irUrl);
                ps.setString(15, adres);
                ps.setString(16, stockCode);
                ps.setString(17, bizrNo);
                ps.setString(18, faxNo);
                ps.setString(19, status);

                int result = ps.executeUpdate();

                if (result > 0) {
                    System.out.println("[SUCCESS] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> MERGE 처리");
                } else {
                    System.out.println("[NO CHANGE] CORP_CODE : " + corpCode + ", CORP_NAME : " + corpName + " -> 변경 없음");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}


