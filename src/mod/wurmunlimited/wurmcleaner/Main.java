package mod.wurmunlimited.wurmcleaner;

import java.sql.*;
import java.util.*;

public class Main {

    private static String pre = "jdbc:sqlite:";
    private static int highestWurmCreatureTemplateId = 116;
    private static Set<String> templatesToDelete = new HashSet<>();

    public static void main(String[] args) {
        if (args.length > 0 && args[0].startsWith("-")) {
            if (args[0].equals("-remove-modded-dens")) {
                deleteHighIdDens();
                return;
            }
        }

        String argString = String.join(" ", args);
        for (String arg : argString.split(",")) {
            templatesToDelete.add(arg.trim().toLowerCase());
        }

        Connection creatures = null;
        Connection items = null;
        Connection players = null;
        try {
            creatures = DriverManager.getConnection(pre + "wurmcreatures.db");
            items = DriverManager.getConnection(pre + "wurmitems.db");
            players = DriverManager.getConnection(pre + "wurmplayers.db");

            findCreatures(creatures, items, players);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (creatures != null)
                    creatures.close();
                if (items != null)
                    items.close();
                if (players != null)
                    players.close();
            } catch (SQLException e1) {
                System.out.println("Could not connect to database.");
                e1.printStackTrace();
            }
        }
    }

    private static void findCreatures(Connection creatures, Connection items, Connection players) throws SQLException {
        Set<Long> ids = new HashSet<>();
        Set<Integer> missionIds = new HashSet<>();
        PreparedStatement search = creatures.prepareStatement("SELECT * FROM CREATURES");
        PreparedStatement missionSearch = players.prepareStatement("SELECT * FROM MISSIONTRIGGERS WHERE ONTARGET=?");
        ResultSet rs = search.executeQuery();

        List<PreparedStatement> statements = Arrays.asList(
                creatures.prepareStatement("DELETE FROM CREATURES WHERE WURMID=?"),
                creatures.prepareStatement("DELETE FROM SKILLS WHERE OWNER=?"),
                creatures.prepareStatement("DELETE FROM PROTECTED WHERE WURMID=?"),
                creatures.prepareStatement("DELETE FROM POSITION WHERE WURMID=?"),
                creatures.prepareStatement("DELETE FROM BRANDS WHERE WURMID=?"),

                items.prepareStatement("DELETE FROM BODYPARTS WHERE OWNERID=?"),
                items.prepareStatement("DELETE FROM ITEMS WHERE OWNERID=?")
        );

        List<PreparedStatement> missionStatements = Arrays.asList(
                players.prepareStatement("DELETE FROM TRIGGEREFFECTS WHERE TRIGGERID=?"),
                players.prepareStatement("DELETE FROM TRIGGERS2EFFECTS WHERE TRIGGERID=?"),
                players.prepareStatement("DELETE FROM MISSIONTRIGGERS WHERE ID=?")
        );

        while (rs.next()) {
            String templateName = rs.getString("TEMPLATENAME");
            if (templatesToDelete.contains(templateName.toLowerCase())) {
                long wurmId = rs.getLong("WURMID");
                ids.add(wurmId);

                missionSearch.setLong(1, wurmId);
                ResultSet mrs = missionSearch.executeQuery();
                while (mrs.next()) {
                    if (ids.contains(mrs.getLong("ONTARGET")))
                        missionIds.add(mrs.getInt("ID"));
                }
            }
        }

        creatures.setAutoCommit(false);
        items.setAutoCommit(false);
        players.setAutoCommit(false);
        for (Long id : ids) {
            for (PreparedStatement ps : statements) {
                ps.setLong(1, id);
                ps.executeUpdate();
            }

            System.out.println("Deleting records for " + id);
        }

        System.out.println("Deleting Mission Triggers.");
        for (Integer id : missionIds) {
            for (PreparedStatement ps : missionStatements) {
                ps.setInt(1, id);
                ps.executeUpdate();
            }
        }

        System.out.println("Committing to database.");
        creatures.commit();
        items.commit();
        players.commit();
    }

    private static void deleteHighIdDens() {
        try {
            Connection zones = DriverManager.getConnection(pre + "wurmzones.db");
            PreparedStatement dens = zones.prepareStatement("SELECT TEMPLATEID FROM DENS");
            PreparedStatement remove = zones.prepareStatement("DELETE FROM DENS WHERE TEMPLATEID=?");
            ResultSet rs = dens.executeQuery();

            System.out.println("Removing modded dens.");
            zones.setAutoCommit(false);
            while (rs.next()) {
                int id = rs.getInt(1);
                if (id > highestWurmCreatureTemplateId) {
                    remove.setInt(1, id);
                    remove.executeUpdate();
                }
            }

            System.out.println("Committing to database.");
            zones.commit();
        } catch (SQLException e) {
            System.out.println("Error when removing dens.");
            e.printStackTrace();
        }
    }
}
