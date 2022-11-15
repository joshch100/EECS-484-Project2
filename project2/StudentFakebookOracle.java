package project2;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;

/*
    The StudentFakebookOracle class is derived from the FakebookOracle class and implements
    the abstract query functions that investigate the database provided via the <connection>
    parameter of the constructor to discover specific information.
*/
public final class StudentFakebookOracle extends FakebookOracle {
    // [Constructor]
    // REQUIRES: <connection> is a valid JDBC connection
    public StudentFakebookOracle(Connection connection) {
        oracle = connection;
    }

    @Override
    // Query 0
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the total number of users for which a birth month is listed
    //        (B) Find the birth month in which the most users were born
    //        (C) Find the birth month in which the fewest users (at least one) were born
    //        (D) Find the IDs, first names, and last names of users born in the month
    //            identified in (B)
    //        (E) Find the IDs, first names, and last name of users born in the month
    //            identified in (C)
    //
    // This query is provided to you completed for reference. Below you will find the appropriate
    // mechanisms for opening up a statement, executing a query, walking through results, extracting
    // data, and more things that you will need to do for the remaining nine queries
    public BirthMonthInfo findMonthOfBirthInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            // Step 1
            // ------------
            // * Find the total number of users with birth month info
            // * Find the month in which the most users were born
            // * Find the month in which the fewest (but at least 1) users were born
            ResultSet rst = stmt.executeQuery(
                    "SELECT COUNT(*) AS Birthed, Month_of_Birth " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth IS NOT NULL " + // for which a birth month is available
                            "GROUP BY Month_of_Birth " + // group into buckets by birth month
                            "ORDER BY Birthed DESC, Month_of_Birth ASC"); // sort by users born in that month, descending; break ties by birth month

            int mostMonth = 0;
            int leastMonth = 0;
            int total = 0;
            while (rst.next()) { // step through result rows/records one by one
                if (rst.isFirst()) { // if first record
                    mostMonth = rst.getInt(2); //   it is the month with the most
                }
                if (rst.isLast()) { // if last record
                    leastMonth = rst.getInt(2); //   it is the month with the least
                }
                total += rst.getInt(1); // get the first field's value as an integer
            }
            BirthMonthInfo info = new BirthMonthInfo(total, mostMonth, leastMonth);

            // Step 2
            // ------------
            // * Get the names of users born in the most popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + mostMonth + " " + // born in the most popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addMostPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 3
            // ------------
            // * Get the names of users born in the least popular birth month
            rst = stmt.executeQuery(
                    "SELECT User_ID, First_Name, Last_Name " + // select ID, first name, and last name
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE Month_of_Birth = " + leastMonth + " " + // born in the least popular birth month
                            "ORDER BY User_ID"); // sort smaller IDs first

            while (rst.next()) {
                info.addLeastPopularBirthMonthUser(new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3)));
            }

            // Step 4
            // ------------
            // * Close resources being used
            rst.close();
            stmt.close(); // if you close the statement first, the result set gets closed automatically

            return info;

        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new BirthMonthInfo(-1, -1, -1);
        }
    }

    @Override
    // Query 1
    // -----------------------------------------------------------------------------------
    // GOALS: (A) The first name(s) with the most letters
    //        (B) The first name(s) with the fewest letters
    //        (C) The first name held by the most users
    //        (D) The number of users whose first name is that identified in (C)
    public FirstNameInfo findNameInfo() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                   "SELECT LENGTH(First_Name) AS Letter, First_Name " + // select birth months and number of uses with that birth month
                            "FROM " + UsersTable + " " + // from all users
                            "WHERE First_Name IS NOT NULL " + // for which a birth month is available
                            "GROUP BY First_Name " + // group into buckets by birth month
                            "ORDER BY Letter DESC, First_Name");
        
            int longest = 0;
            int shortest = 0;
            FirstNameInfo info = new FirstNameInfo();

            while (rst.next()) { // step through result rows/records one by one
                String str = rst.getString(2);
                //System.out.println(str + "/n");
                if (rst.isFirst()) { // if first record
                    longest = str.length();
                }
                if (rst.isLast()) { // if last record
                    shortest = str.length();
                }
            }
            ResultSet rst2 = stmt.executeQuery(
                   "SELECT First_Name " + 
                            "FROM " + UsersTable + " " + 
                            "WHERE First_Name IS NOT NULL " + 
                            "GROUP BY First_Name " + 
                            "ORDER BY First_Name ASC");

            while(rst2.next()){
                String str = rst2.getString(1);
                //System.out.println(str + "/n");
                if(str.length() == longest){
                    info.addLongName(str);
                }else if(str.length() == shortest){
                    info.addShortName(str);
                }
            }
            int qunatity = 0;
            ResultSet rst3 = stmt.executeQuery(
                   "SELECT First_Name, COUNT(First_Name) " + 
                            "FROM " + UsersTable + " " + 
                            "WHERE First_Name IS NOT NULL " + 
                            "GROUP BY First_Name " + 
                            "ORDER BY COUNT(First_Name) DESC, First_Name");   
            while (rst3.next()) { 
                if (rst3.isFirst()) { // if first record
                    qunatity = rst3.getInt(2);
                    info.setCommonNameCount(qunatity);
                }
            }
            ResultSet rst4 = stmt.executeQuery(
                   "SELECT First_Name, COUNT(First_Name) " + 
                            "FROM " + UsersTable + " " + 
                            "WHERE First_Name IS NOT NULL " + 
                            "GROUP BY First_Name " + 
                            "ORDER BY First_Name ASC, COUNT(First_Name)");
            while(rst4.next()){
                String str = rst4.getString(1);
                int ammount = rst4.getInt(2);
                //System.out.println(str + "/n");
                if(ammount == qunatity){
                    info.addCommonName(str);
                }
            }
            return info;

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FirstNameInfo info = new FirstNameInfo();
                info.addLongName("Aristophanes");
                info.addLongName("Michelangelo");
                info.addLongName("Peisistratos");
                info.addShortName("Bob");
                info.addShortName("Sue");
                info.addCommonName("Harold");
                info.addCommonName("Jessica");
                info.setCommonNameCount(42);
                return info;
            */
            //return new FirstNameInfo(); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new FirstNameInfo();
        }
    }

    @Override
    // Query 2
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users without any friends
    //
    // Be careful! Remember that if two users are friends, the Friends table only contains
    // the one entry (U1, U2) where U1 < U2.
    public FakebookArrayList<UserInfo> lonelyUsers() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            ResultSet rst = stmt.executeQuery(
                 "SELECT DISTINCT User_ID, First_Name, Last_Name " + 
                            "FROM " + UsersTable + " " + 
                            "WHERE (User_ID NOT IN " +
                            "(SELECT USER1_ID FROM " + FriendsTable + ")) " + 
                            "AND (User_ID NOT IN" +
                            "( SELECT USER2_ID FROM " + FriendsTable + " )) " + 
                            "ORDER BY User_ID ASC");
                
            while(rst.next()){
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                FriendsTable
                UserInfo u1 = new UserInfo(15, "Abraham", "Lincoln");
                UserInfo u2 = new UserInfo(39, "Margaret", "Thatcher");
                results.add(u1);
                results.add(u2);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        return results;
    }

    @Override
    // Query 3
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of users who no longer live
    //            in their hometown (i.e. their current city and their hometown are different)
    public FakebookArrayList<UserInfo> liveAwayFromHome() throws SQLException {
        FakebookArrayList<UserInfo> results = new FakebookArrayList<UserInfo>(", ");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            ResultSet rst = stmt.executeQuery(
                 "SELECT DISTINCT U.User_ID, U.First_Name, U.Last_Name " + 
                            "FROM " + UsersTable + " U JOIN " + CurrentCitiesTable + " C ON U.USER_ID = C.USER_ID " +
                            "JOIN " + HometownCitiesTable + " H ON U.USER_ID = H.USER_ID " +
                            "WHERE C.CURRENT_CITY_ID != H.HOMETOWN_CITY_ID " +
                            "ORDER BY U.User_ID ASC");
                
            while(rst.next()){
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                results.add(u1);
            }
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                CurrentCitiesTable
                HometownCitiesTable
                UserInfo u1 = new UserInfo(9, "Meryl", "Streep");
                UserInfo u2 = new UserInfo(104, "Tom", "Hanks");
                results.add(u1);
                results.add(u2);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

   
@Override
    // Query 4
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, links, and IDs and names of the containing album of the top
    //            <num> photos with the most tagged users
    //        (B) For each photo identified in (A), find the IDs, first names, and last names
    //            of the users therein tagged
    public FakebookArrayList<TaggedPhotoInfo> findPhotosWithMostTags(int num) throws SQLException {
        FakebookArrayList<TaggedPhotoInfo> results = new FakebookArrayList<TaggedPhotoInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            
            ResultSet rst2 = stmt.executeQuery(
                "SELECT DISTINCT P.Photo_ID, P.Photo_link, U.USER_ID, U.last_name, A.Album_ID, A.Album_Name, U.first_name, T.TAG_SUBJECT_ID, COUNT(T.TAG_SUBJECT_ID) AS SUM " + 
                "FROM "+ TagsTable + " T JOIN " + PhotosTable +
                " P ON P.photo_id = T.tag_photo_id JOIN " + UsersTable +
                " U ON T.TAG_SUBJECT_ID = U.USER_ID Join " + AlbumsTable + " A ON P.Album_ID = A.Album_ID" +
                " WHERE P.Photo_ID IN " +
                "(SELECT * FROM (SELECT DISTINCT P1.Photo_ID " + 
                            "FROM " + PhotosTable + " P1 JOIN " + AlbumsTable + 
                            " A1 ON P1.Album_ID = A1.Album_ID JOIN " + TagsTable +
                            " T1 ON T1.TAG_PHOTO_ID = P1.Photo_ID " +
                            "GROUP BY  P1.Photo_ID" +
                            " ORDER BY COUNT(T1.TAG_SUBJECT_ID) DESC, P1.Photo_ID ASC " + 
                            " ) WHERE ROWNUM <= " + (num + 1) + " ) " +
                " GROUP BY P.Photo_ID, P.Photo_link, U.USER_ID, U.last_name, A.Album_ID, A.Album_Name, U.first_name, T.TAG_SUBJECT_ID " +
                " ORDER BY SUM DESC, P.Photo_ID ASC, T.TAG_SUBJECT_ID ASC");
            int photo_temp_id = 0;
            PhotoInfo d = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
            TaggedPhotoInfo tp = new TaggedPhotoInfo(d);

            while(rst2.next()){
                if (rst2.isFirst()) { 
                        PhotoInfo p = new PhotoInfo(rst2.getInt(1), rst2.getInt(5), rst2.getString(2), rst2.getString(6));
                        tp = new TaggedPhotoInfo(p);
                        UserInfo u1 = new UserInfo(rst2.getInt(8), rst2.getString(7), rst2.getString(4));
                        tp.addTaggedUser(u1);
                        photo_temp_id = rst2.getInt(1);
                }else if(photo_temp_id != rst2.getInt(1)){
                        photo_temp_id = rst2.getInt(1);
                        results.add(tp);
                        PhotoInfo p = new PhotoInfo(rst2.getInt(1), rst2.getInt(5), rst2.getString(2), rst2.getString(6));
                        tp = new TaggedPhotoInfo(p);
                        UserInfo u1 = new UserInfo(rst2.getInt(8), rst2.getString(7), rst2.getString(4));
                        tp.addTaggedUser(u1);
                }else{
                        UserInfo u1 = new UserInfo(rst2.getInt(8), rst2.getString(7), rst2.getString(4));
                        tp.addTaggedUser(u1);
                    }
            }
            
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                PhotoInfo p = new PhotoInfo(80, 5, "www.photolink.net", "Winterfell S1");
                UserInfo u1 = new UserInfo(3901, "Jon", "Snow");
                UserInfo u2 = new UserInfo(3902, "Arya", "Stark");
                UserInfo u3 = new UserInfo(3903, "Sansa", "Stark");
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
                tp.addTaggedUser(u1);
                tp.addTaggedUser(u2);
                tp.addTaggedUser(u3);
                results.add(tp);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }


    @Override
    // Query 5
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, last names, and birth years of each of the two
    //            users in the top <num> pairs of users that meet each of the following
    //            criteria:
    //              (i) same gender
    //              (ii) tagged in at least one common photo
    //              (iii) difference in birth years is no more than <yearDiff>
    //              (iv) not friends
    //        (B) For each pair identified in (A), find the IDs, links, and IDs and names of
    //            the containing album of each photo in which they are tagged together
    public FakebookArrayList<MatchPair> matchMaker(int num, int yearDiff) throws SQLException {
        FakebookArrayList<MatchPair> results = new FakebookArrayList<MatchPair>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

                ResultSet rst2 = stmt.executeQuery(
                "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, T1.TAG_PHOTO_ID, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH, " + 
                "P.PHOTO_ID, P.PHOTO_LINK, A.ALBUM_ID, A.ALBUM_NAME, " +
                "COUNT(T1.TAG_PHOTO_ID) AS COUNT FROM " + UsersTable + 
                " U1, " + UsersTable +  " U2, " + TagsTable+  " T1, "+ TagsTable+ " T2, " + AlbumsTable + " A, " + PhotosTable + " P " +
                "WHERE ABS(U2.Year_of_birth - U1.Year_of_birth) <= " + yearDiff + "AND U2.Gender = U1.Gender AND U1.USER_ID < U2.USER_ID " +
                "AND U1.USER_ID = T1.TAG_SUBJECT_ID AND U2.USER_ID = T2.TAG_SUBJECT_ID AND T1.TAG_PHOTO_ID = T2.TAG_PHOTO_ID " +
                "AND T1.TAG_PHOTO_ID = P.PHOTO_ID AND P.ALBUM_ID = A.ALBUM_ID " +
                "AND U1.USER_ID NOT IN " +
                "(SELECT U1.USER_ID FROM " + FriendsTable +  " F WHERE U1.USER_ID = F.USER1_ID AND U2.USER_ID = F.USER2_ID) "+
                "AND U1.USER_ID < U2.USER_ID " + 
                "GROUP BY U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U1.YEAR_OF_BIRTH, T1.TAG_PHOTO_ID, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME, U2.YEAR_OF_BIRTH, " + 
                "P.PHOTO_ID, P.PHOTO_LINK, A.ALBUM_ID, A.ALBUM_NAME " +
                "ORDER BY COUNT DESC, U1.USER_ID ASC");
            
            for(int i = 0; i < num ; ++i){
                if(rst2.next()){
                    // System.out.println(rst2.getInt(1));
                    // System.out.println(rst2.getString(2));
                    UserInfo u1 = new UserInfo(rst2.getInt(1), rst2.getString(2), rst2.getString(3));
                    UserInfo u2 = new UserInfo(rst2.getInt(6), rst2.getString(7), rst2.getString(8));
                    MatchPair mp = new MatchPair(u1, rst2.getInt(4), u2, rst2.getInt(9));
                    PhotoInfo p = new PhotoInfo(rst2.getInt(5), rst2.getInt(12), rst2.getString(11), rst2.getString(13));
                    mp.addSharedPhoto(p);
                    results.add(mp);
                }
            }
         

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(93103, "Romeo", "Montague");
                UserInfo u2 = new UserInfo(93113, "Juliet", "Capulet");
                MatchPair mp = new MatchPair(u1, 1597, u2, 1597);
                PhotoInfo p = new PhotoInfo(167, 309, "www.photolink.net", "Tragedy");
                mp.addSharedPhoto(p);
                results.add(mp);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 6
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the IDs, first names, and last names of each of the two users in
    //            the top <num> pairs of users who are not friends but have a lot of
    //            common friends
    //        (B) For each pair identified in (A), find the IDs, first names, and last names
    //            of all the two users' common friends
    public FakebookArrayList<UsersPair> suggestFriends(int num) throws SQLException {
        FakebookArrayList<UsersPair> results = new FakebookArrayList<UsersPair>("\n");

        

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {

            // System.out.println("hey1");
      

            // System.out.println("heyx");

            stmt.executeUpdate(
                "CREATE VIEW Case1 AS "+
                "SELECT F1.USER1_ID AS USERID1 , F2.USER1_ID AS USERID2, F1.USER2_ID AS FriendID " +
                "FROM " + FriendsTable + " F1 " +
                "JOIN " + FriendsTable + " F2 ON F1.USER1_ID < F2.USER1_ID AND F1.USER2_ID = F2.USER2_ID " +
                "WHERE NOT EXISTS (SELECT 1 FROM " + FriendsTable + " F3 WHERE F1.USER1_ID = F3.USER1_ID AND F2.USER1_ID = F3.USER2_ID)");       


            // System.out.println("hey2");
            // System.out.println("heyx");


            stmt.executeUpdate(
                "CREATE VIEW Case2 AS " +
                "SELECT F1.USER2_ID AS USERID1 , F2.USER2_ID AS USERID2, F1.USER1_ID AS FriendID " +
                "FROM " + FriendsTable + " F1 " +
                "JOIN " + FriendsTable + " F2 ON F1.USER1_ID = F2.USER1_ID AND F1.USER2_ID < F2.USER2_ID " +
                "WHERE NOT EXISTS (SELECT 1 FROM " + FriendsTable + " F3 WHERE F1.USER2_ID = F3.USER1_ID AND F2.USER2_ID = F3.USER2_ID)"); 

            // System.out.println("hey3");

            stmt.executeUpdate(
                "CREATE VIEW Case3 AS " +
                "SELECT F1.USER1_ID AS USERID1 , F2.USER2_ID AS USERID2, F1.USER2_ID AS FriendID " +
                "FROM " + FriendsTable + " F1 " +
                "JOIN " + FriendsTable + " F2 ON F1.USER2_ID = F2.USER1_ID AND F1.USER1_ID < F2.USER2_ID " +
                "WHERE NOT EXISTS (SELECT 1 FROM " + FriendsTable + " F3 WHERE F1.USER1_ID = F3.USER1_ID AND F2.USER2_ID = F3.USER2_ID)"); 

            // System.out.println("hey4");

            // stmt.executeUpdate("DROP VIEW UsercommonAndFriends");

            stmt.executeUpdate(
                "CREATE VIEW UsercommonAndFriends AS " +
                    "SELECT * FROM Case1 " +
                    "UNION " +
                    "SELECT * FROM Case2 " +
                    "UNION " +
                    "SELECT * FROM Case3"); 
            
            // System.out.println("hey5");

            // stmt.executeUpdate(
            //         "DROP VIEW UsercommonCOUNT");
            // stmt.executeUpdate(
            //         "DROP VIEW UsercommonCOUNT");

            stmt.executeUpdate(
                "CREATE VIEW UsercommonCOUNT AS " +
                    "SELECT  USERID1 , USERID2, COUNT(*) AS NumofFriends " +
                    "FROM UsercommonAndFriends " +
                    "GROUP BY USERID1, USERID2 " +
                    "ORDER BY COUNT(*) DESC, USERID1, USERID2"); 

            // (A) Find the IDs, first names, and last names of each of the two users in
            //     //            the top <num> pairs of users who are not friends but have a lot of
            //     //            common friends
            // System.out.println("hey2");

            ResultSet rst = stmt.executeQuery(
                    "SELECT USERID1, USER1FNAME, USER1LNAME, USERID2, USER2FNAME, USER2LNAME, NumofFriends FROM " +
                    "(SELECT USERID1, U1.FIRST_NAME AS USER1FNAME, U1.LAST_NAME AS USER1LNAME, USERID2, U2.FIRST_NAME AS USER2FNAME, U2.LAST_NAME AS USER2LNAME, NumofFriends " +
                    "FROM UsercommonCOUNT LEFT JOIN " + UsersTable + " U1 ON U1.USER_ID = USERID1 LEFT JOIN " + UsersTable+  " U2 ON U2.USER_ID = USERID2) " +
                    "WHERE ROWNUM <= " + num);
            
            
            Statement stmt2 = oracle.createStatement(FakebookOracleConstants.AllScroll, FakebookOracleConstants.ReadOnly);

            // stmt2.executeUpdate(
            //         "DROP VIEW PartA");

            // stmt2.executeUpdate(
            //         "CREATE VIEW PartA AS " +
            //         "SELECT USERID1, USER1FNAME, USER1LNAME, USERID2, USER2FNAME, USER2LNAME, NumofFriends FROM " +
            //         "(SELECT USERID1, U1.FIRST_NAME AS USER1FNAME, U1.LAST_NAME AS USER1LNAME, USERID2, U2.FIRST_NAME AS USER2FNAME, U2.LAST_NAME AS USER2LNAME, NumofFriends " +
            //         "FROM UsercommonCOUNT LEFT JOIN " + UsersTable + " U1 ON U1.USER_ID = USERID1 LEFT JOIN " + UsersTable+  " U2 ON U2.USER_ID = USERID2) " +
            //         "WHERE ROWNUM <= " + num);

            
            
            // System.out.println("hey3");
            // System.out.println("hey5");
            while (rst.next()) { // step through result rows/records one by one
                UserInfo u1 = new UserInfo(rst.getInt(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getInt(4), rst.getString(5), rst.getString(6));
                UsersPair up = new UsersPair(u1, u2);
                
                ResultSet rst2= stmt2.executeQuery(
                    "SELECT U.USER_ID, U.FIRST_NAME, U.LAST_NAME FROM " +
                    "UsercommonAndFriends UAF LEFT JOIN  "  + UsersTable + " U ON UAF.FriendID = U.USER_ID " +
                    "WHERE UAF.USERID1 = " + rst.getInt(1)  + " AND  UAF.USERID2 = " + rst.getInt(4) + " " +
                    "ORDER BY U.USER_ID");

                while (rst2.next()) {
                    UserInfo u3 = new UserInfo(rst2.getInt(1), rst2.getString(2), rst2.getString(3));
                    up.addSharedFriend(u3);
                }
            
                results.add(up);
            }

            stmt.executeUpdate("DROP VIEW UsercommonAndFriends");
            stmt.executeUpdate("DROP VIEW UsercommonCOUNT");
            stmt.executeUpdate("DROP VIEW Case1");
            stmt.executeUpdate("DROP VIEW Case2");
            stmt.executeUpdate("DROP VIEW Case3");

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(16, "The", "Hacker");
                UserInfo u2 = new UserInfo(80, "Dr.", "Marbles");
                UserInfo u3 = new UserInfo(192, "Digit", "Le Boid");
                UsersPair up = new UsersPair(u1, u2);
                up.addSharedFriend(u3);
                results.add(up);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    @Override
    // Query 7
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the name of the state or states in which the most events are held
    //        (B) Find the number of events held in the states identified in (A)
    public EventStateInfo findEventStates() throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            ResultSet rst = stmt.executeQuery(
                "SELECT T.STATE_NAME, T.NUM_EVENTS FROM " +
                "(SELECT C.STATE_NAME, COUNT(*) AS NUM_EVENTS FROM " + EventsTable + " E, " + CitiesTable + " C " +
                "WHERE E.EVENT_CITY_ID = C.CITY_ID GROUP BY C.STATE_NAME) T " +
                "WHERE T.NUM_EVENTS >= ALL (SELECT COUNT(*) FROM " + EventsTable + " E1, " + CitiesTable + " C1 " +
                "WHERE E1.EVENT_CITY_ID = C1.CITY_ID GROUP BY C1.STATE_NAME)");

            int temp = 0;
            String temp2 = " ";
            while(rst.next()){
                temp = rst.getInt(2);
                temp2 = rst.getString(1);
            }
            EventStateInfo info = new EventStateInfo(temp);
            info.addState(temp2);


            return info;
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                EventStateInfo info = new EventStateInfo(50);
                info.addState("Kentucky");
                info.addState("Hawaii");
                info.addState("New Hampshire");
                return info;
            */
            //return new EventStateInfo(-1); // placeholder for compilation
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new EventStateInfo(-1);
        }
    }

    @Override
    // Query 8
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find the ID, first name, and last name of the oldest friend of the user
    //            with User ID <userID>
    //        (B) Find the ID, first name, and last name of the youngest friend of the user
    //            with User ID <userID>
    public AgeInfo findAgeInfo(long userID) throws SQLException {
        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            //Oldest
            ResultSet rst = stmt.executeQuery(
                "SELECT * FROM (SELECT F.USER2_ID, U.FIRST_NAME, U.LAST_NAME "+
                "FROM " + FriendsTable + " F, " + UsersTable + " U " +
                "WHERE USER1_ID = " + userID + " AND U.USER_ID = F.USER2_ID " +
                "ORDER BY U.YEAR_OF_BIRTH, U.MONTH_OF_BIRTH, U.DAY_OF_BIRTH, F.USER2_ID DESC) "+ 
                "WHERE ROWNUM = 1");
            int temp = 0;
            String t2 = "", t3 = "";

            while(rst.next()){
                temp = rst.getInt(1);
                t2 = rst.getString(2);
                t3 = rst.getString(3);
            }
            UserInfo old = new UserInfo(temp, t2, t3);

            //Youngest
            rst = stmt.executeQuery(
                "SELECT * FROM (SELECT F.USER2_ID, U.FIRST_NAME, U.LAST_NAME "+
                "FROM " + FriendsTable + " F, " + UsersTable + " U " +
                "WHERE USER1_ID = " + userID + " AND U.USER_ID = F.USER2_ID " +
                "ORDER BY U.YEAR_OF_BIRTH DESC, U.MONTH_OF_BIRTH DESC, U.DAY_OF_BIRTH DESC, F.USER2_ID DESC) "+ 
                "WHERE ROWNUM = 1");    
            while(rst.next()){
                temp = rst.getInt(1);
                t2 = rst.getString(2);
                t3 = rst.getString(3);
            }
            UserInfo young = new UserInfo(temp, t2, t3);

            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo old = new UserInfo(12000000, "Galileo", "Galilei");
                UserInfo young = new UserInfo(80000000, "Neil", "deGrasse Tyson");
                return new AgeInfo(old, young);
            */
            return new AgeInfo(old,young); // placeholder for compilation
            //return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return new AgeInfo(new UserInfo(-1, "ERROR", "ERROR"), new UserInfo(-1, "ERROR", "ERROR"));
        }
    }

    @Override
    // Query 9
    // -----------------------------------------------------------------------------------
    // GOALS: (A) Find all pairs of users that meet each of the following criteria
    //              (i) same last name
    //              (ii) same hometown
    //              (iii) are friends
    //              (iv) less than 10 birth years apart
    public FakebookArrayList<SiblingInfo> findPotentialSiblings() throws SQLException {
        FakebookArrayList<SiblingInfo> results = new FakebookArrayList<SiblingInfo>("\n");

        try (Statement stmt = oracle.createStatement(FakebookOracleConstants.AllScroll,
                FakebookOracleConstants.ReadOnly)) {
            ResultSet rst = stmt.executeQuery(
                "SELECT U1.USER_ID, U1.FIRST_NAME, U1.LAST_NAME, U2.USER_ID, U2.FIRST_NAME, U2.LAST_NAME " + 
                "FROM " + UsersTable+ " U1, " + UsersTable+ " U2, " + HometownCitiesTable + " H1," +
                HometownCitiesTable + " H2, " + FriendsTable + " F "+
                "WHERE F.USER1_ID = U1.USER_ID AND F.USER2_ID = U2.USER_ID AND F.USER1_ID = H1.USER_ID AND F.USER2_ID = H2.USER_ID "+
                "AND H1.HOMETOWN_CITY_ID = H2.HOMETOWN_CITY_ID AND U1.LAST_NAME = U2.LAST_NAME " +
                "AND ((U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) < 10 AND (U1.YEAR_OF_BIRTH - U2.YEAR_OF_BIRTH) > -10) " +
                "ORDER BY U1.USER_ID, U2.USER_ID");

            while(rst.next()){
                UserInfo u1 = new UserInfo(rst.getLong(1), rst.getString(2), rst.getString(3));
                UserInfo u2 = new UserInfo(rst.getLong(4), rst.getString(5), rst.getString(6));
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            }

            rst.close();
            stmt.close();
            /*
                EXAMPLE DATA STRUCTURE USAGE
                ============================================
                UserInfo u1 = new UserInfo(81023, "Kim", "Kardashian");
                UserInfo u2 = new UserInfo(17231, "Kourtney", "Kardashian");
                SiblingInfo si = new SiblingInfo(u1, u2);
                results.add(si);
            */
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return results;
    }

    // Member Variables
    private Connection oracle;
    private final String UsersTable = FakebookOracleConstants.UsersTable;
    private final String CitiesTable = FakebookOracleConstants.CitiesTable;
    private final String FriendsTable = FakebookOracleConstants.FriendsTable;
    private final String CurrentCitiesTable = FakebookOracleConstants.CurrentCitiesTable;
    private final String HometownCitiesTable = FakebookOracleConstants.HometownCitiesTable;
    private final String ProgramsTable = FakebookOracleConstants.ProgramsTable;
    private final String EducationTable = FakebookOracleConstants.EducationTable;
    private final String EventsTable = FakebookOracleConstants.EventsTable;
    private final String AlbumsTable = FakebookOracleConstants.AlbumsTable;
    private final String PhotosTable = FakebookOracleConstants.PhotosTable;
    private final String TagsTable = FakebookOracleConstants.TagsTable;
}

