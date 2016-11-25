package iamanidiot.dbupgradeapp;

/**
 * Created by iamanidiot on 4/7/16.
 */

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.support.design.widget.TabLayout;
import android.util.Log;

/**
 * Created by iamanidiot on 26/5/16.
 */
public class DataConnect extends SQLiteOpenHelper
{
    public static String DATABASE_NAME = "DBUpgradeTest.db";
    public static int DATABASE_VERSION = 1;
    // Table Definition Constants: Activity Table
    public static String DATABASE_TABLE_Activity = "Activities";
    public static String COLUMN_ACTIVITY_TABLE_id = "Activity_id";
    public static String COLUMN_ACTIVITY_TABLE_ActivityName = "activity_name";
    public static String COLUMN_ACTIVITY_TABLE_PseudoName = "pseudo_name";
    public static String COLUMN_ACTIVITY_TABLE_isActive = "isActive";
    public static String COLUMN_ACTIVITY_TABLE_Stamp = "Stamp";
    public static String COLUMN_ACTIVITY_TABLE_Notes = "Notes";

    // Table Definition Constants: Occurence Table
    public static String DATABASE_TABLE_Occurence = "Occurences";
    public static String COLUMN_OCCURENCE_TABLE_id = "Occurence_Activity_id";
    public static String COLUMN_OCCURENCE_TABLE_OccuredAt = "Occured_at";
    public static String COLUMN_OCCURENCE_TABLE_isCurrent = "isCurrent";
    public static String COLUMN_OCCURENCE_TABLE_Description = "Description";
    public static String COLUMN_OCCURENCE_TABLE_Stamp = "Stamp";
    public static String COLUMN_OCCURENCE_TABLE_Notes = "Notes";

    //LOG TAG Defns:
    public static String LOG_TAG_DB_OCC = "OccurenceDataConnect";
    public static String LOG_TAG_DB_ACT = "ActivityDataConnect";
    public static String LOG_TAG_DB_STATS_SINGLE = "StatsSingleActivity";
    public static String LOG_TAG_DB_HOMESCREEN_RECENTACT = "HOMESCREEN_RECENTACT";

    private SQLiteDatabase ActivityDB;

    public DataConnect(Context context)
    {
        this(context,DATABASE_NAME,null,DATABASE_VERSION);
    }

    public DataConnect(Context context, String name, SQLiteDatabase.CursorFactory factory, int version)
    {
        super(context, name, factory, version);
    }

    @Override
    public void onCreate(SQLiteDatabase db)
    {
        Log.e(LOG_TAG_DB_ACT, "Inside Create Method");
        //
        //// Activity Table Create Statement
        //
        String CREATE_ACTIVITY_TABLE = "CREATE TABLE " +
                DATABASE_TABLE_Activity + " ( " +
                COLUMN_ACTIVITY_TABLE_id + " INTEGER PRIMARY KEY ASC AUTOINCREMENT, " +
                COLUMN_ACTIVITY_TABLE_ActivityName + " STRING (30), " +
                COLUMN_ACTIVITY_TABLE_PseudoName + " STRING, " +
                COLUMN_ACTIVITY_TABLE_isActive + " BOOLEAN DEFAULT TRUE, " +
                COLUMN_ACTIVITY_TABLE_Notes + "STRING (50)" +
                COLUMN_ACTIVITY_TABLE_Stamp + " DATETIME DEFAULT (CURRENT_TIMESTAMP));";

        //
        //// ActivitiesGroup View Create Statement
        //
/*        String allActivitiesGroupedView = "CREATE VIEW ALL_ACTIVITIES_COUNT_GROUPED AS SELECT " +
                COLUMN_ACTIVITY_TABLE_id + ", " + COLUMN_ACTIVITY_TABLE_ActivityName + ", count(*)" +
                " FROM " + DATABASE_TABLE_Occurence + " O " +
                "INNER JOIN " +
                DATABASE_TABLE_Activity + "A" +
                "ON O." + COLUMN_OCCURENCE_TABLE_id + " = " + "A." + COLUMN_ACTIVITY_TABLE_id+
                "GROUP BY " + "O." + COLUMN_OCCURENCE_TABLE_id +";";*/

        //
        //// Occurrence Table Create Statement
        //
        String CREATE_OCCURENCE_TABLE = "CREATE TABLE " +
                DATABASE_TABLE_Occurence + " ( " +
                COLUMN_OCCURENCE_TABLE_id + " INTEGER " +
                " REFERENCES " + DATABASE_TABLE_Activity + "(" + COLUMN_ACTIVITY_TABLE_id +") ON DELETE CASCADE ON UPDATE CASCADE MATCH FULL NOT DEFERRABLE INITIALLY IMMEDIATE, " +
                COLUMN_OCCURENCE_TABLE_OccuredAt + " DATETIME DEFAULT (CURRENT_TIMESTAMP), " +
                COLUMN_OCCURENCE_TABLE_isCurrent + " BOOLEAN DEFAULT TRUE, " +
                COLUMN_OCCURENCE_TABLE_Description + " STRING (50), " +
                COLUMN_OCCURENCE_TABLE_Notes + " STRING (50)" +
                COLUMN_OCCURENCE_TABLE_Stamp + " DATETIME DEFAULT (CURRENT_TIMESTAMP));";

        //// Execute the Create Statements
        try
        {
            if (db.isOpen())
            {
                db.execSQL(CREATE_ACTIVITY_TABLE);
                db.execSQL(CREATE_OCCURENCE_TABLE);
                //db.execSQL(allActivitiesGroupedView);
            }
        }
        catch (SQLException e)
        {
            Log.d(LOG_TAG_DB_ACT,"_*_*_*_*_*_*_*_ DB CREATE STATEMENT _*_*_*_*_*_*_*_\n" + e.getMessage());
        }
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion)
    {
        Log.d(LOG_TAG_DB_ACT, "*********************** Upgrading DB *******************************************\n");
        onCreate(db);
    }
    //
    //// Add new Activity to DB
    //
    public boolean addActivity(String activity, String pseudoName, String stamp)
    {
        try
        {
            ActivityDB = this.getWritableDatabase();
            if (ActivityDB.isOpen())
            {
                Log.e(LOG_TAG_DB_ACT, "DB Open, gonna insert Activity");
                ContentValues insertActivity = new ContentValues();
                insertActivity.put(COLUMN_ACTIVITY_TABLE_ActivityName, activity);
                insertActivity.put(COLUMN_ACTIVITY_TABLE_PseudoName, pseudoName);
                insertActivity.put (COLUMN_ACTIVITY_TABLE_Stamp,stamp);
                long rownum = ActivityDB.insert(DATABASE_TABLE_Activity, null, insertActivity);
                if (rownum >= 0)
                {
                    Log.e(LOG_TAG_DB_ACT, "Value Inserted");
                    ActivityDB.close();
                    return true;
                }
                else
                {
                    ActivityDB.close();
                    return false;
                }
            }
            else
                return false;
        }
        catch (Exception e)
        {
            Log.e(LOG_TAG_DB_ACT,e.getMessage());
            ActivityDB.close();
            return false;
        }
    }

    //
    //// Records Occurence of Activity
    //
    public boolean recordOccurence(int ID,String timestamp,String stamp)//,String description)
    {
        try
        {
            ActivityDB = this.getWritableDatabase ();
            if ( ActivityDB.isOpen () )
            {
                if ( timestamp != null )
                {
                    ContentValues recordOccurence = new ContentValues ();
                    recordOccurence.put (COLUMN_OCCURENCE_TABLE_id, ID);
                    recordOccurence.put (COLUMN_OCCURENCE_TABLE_OccuredAt, timestamp);
                    recordOccurence.put (COLUMN_ACTIVITY_TABLE_Stamp, stamp);
                    long rownum = ActivityDB.insert (DATABASE_TABLE_Occurence, null, recordOccurence);
                    if ( rownum > 0 )
                    {
                        ActivityDB.close ();
                        Log.d (LOG_TAG_DB_OCC, "\n\n_*_*_*_*_*_*_*_*_*_Added  Occurence for ID " + ID + " *_*_*_*_*_*_*_*_*_*_*_*\n\n");
                        return true;
                    } else
                    {
                        ActivityDB.close ();
                        return false;
                    }
                }
            }
        } catch ( SQLException e )
        {
            ActivityDB.close ();
            Log.d (LOG_TAG_DB_OCC, e.getStackTrace ().toString ());
            Log.d (LOG_TAG_DB_OCC, "\n\n_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*\n\n");
            Log.d (LOG_TAG_DB_OCC, e.getMessage ());
            return false;
        } finally
        {
            ActivityDB.close();
            return false;
        }
    }

    public Cursor getStatsForAllActivities()
    {
        try
        {
            ActivityDB = this.getReadableDatabase();
            Cursor returnCursor;
            Log.d(LOG_TAG_DB_OCC,"\n\n_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*\n\n");
            Log.d(LOG_TAG_DB_OCC,"\n\n_*_*_*_*_*_*_*_*_* In ALL Activity Stats DB Func _*_*_*_*_*_*_*_*_*_*_*_*\n\n");
            Log.d(LOG_TAG_DB_OCC,"\n\n_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*\n\n");
            String queryForActivityStats = "SELECT " +
                    "activity." + COLUMN_ACTIVITY_TABLE_id + " AS \"_id\", " +
                    "activity." + COLUMN_ACTIVITY_TABLE_PseudoName + " AS \"Activity\", " +
                    "strftime('%Y-%m-%d'," + COLUMN_OCCURENCE_TABLE_OccuredAt +") as \"Date\", " +
                    "count(activity_id) AS \"Counts\" " +
                    " FROM " +
                    DATABASE_TABLE_Activity + " activity " +
                    " INNER JOIN " +
                    DATABASE_TABLE_Occurence + " occured " +
                    " ON " +
                    " occured.activity_id = activity._activity_id " +
                    " GROUP BY _id,Date " +
                    " ORDER BY _id,Date DESC;";
            returnCursor = ActivityDB.rawQuery(queryForActivityStats,null);
            return returnCursor;
        }
        catch(Exception e)
        {
            Log.d(LOG_TAG_DB_STATS_SINGLE,e.getMessage());
        }
        return null;
    }


    public Cursor homeScreenRecentActs()
    {
        try
        {
            ActivityDB = this.getReadableDatabase();
            if(ActivityDB.isOpen())
            {
                String SelectSQL = "SELECT " +
                        " occ." + COLUMN_OCCURENCE_TABLE_id + " AS \"_id\", " +
                        " act." + COLUMN_ACTIVITY_TABLE_PseudoName + " AS ActivityName, " +
                        " count(occ." + COLUMN_OCCURENCE_TABLE_id + ") AS \"DailyCount\", " +
                        " strftime('%d'," + COLUMN_OCCURENCE_TABLE_OccuredAt + ") AS Date, " +
                        " strftime('%m'," + COLUMN_OCCURENCE_TABLE_OccuredAt + ") AS Month, " +
                        " strftime('%y'," + COLUMN_OCCURENCE_TABLE_OccuredAt + ") AS Year, " +
                        " strftime('%w',occ." + COLUMN_OCCURENCE_TABLE_OccuredAt + ") AS Day " +
                        " FROM " +
                        DATABASE_TABLE_Occurence + " occ " +
                        " INNER JOIN " +
                        DATABASE_TABLE_Activity + " act" +
                        " ON " +
                        " occ." + COLUMN_OCCURENCE_TABLE_id + " = act." + COLUMN_ACTIVITY_TABLE_id +
                    " WHERE " +
                        "Occ." + COLUMN_OCCURENCE_TABLE_id + " in (SELECT distinct " + COLUMN_OCCURENCE_TABLE_id + " from " +
                        DATABASE_TABLE_Occurence + " order by " + COLUMN_OCCURENCE_TABLE_OccuredAt + " desc limit 5) " +
                        //" AND strftime('%d','now') = " +
                        " GROUP BY _id " +
                        " ORDER BY DailyCount DESC, occ." + COLUMN_OCCURENCE_TABLE_OccuredAt + " DESC LIMIT 5";
                Cursor returnCursor = ActivityDB.rawQuery(SelectSQL, null);
                return returnCursor;
            }
            else return null;
        }
        catch (Exception e)
        { Log.e(LOG_TAG_DB_HOMESCREEN_RECENTACT,e.getMessage()); return null; }
    }

    public Cursor getActivityNames()
    {
        ActivityDB = this.getReadableDatabase();
        String queryForAllActivityNames = "SELECT " +
                COLUMN_ACTIVITY_TABLE_id + " AS _id, " +
                COLUMN_ACTIVITY_TABLE_PseudoName + " AS ActivityName " +
                " FROM " + DATABASE_TABLE_Activity +
                " ORDER BY " + COLUMN_ACTIVITY_TABLE_id;
        Cursor returnActivityNames = ActivityDB.rawQuery (queryForAllActivityNames,null);
        return  returnActivityNames;
    }

    public Cursor GetAllActivities()
    {
        try
        {
            ActivityDB = this.getWritableDatabase();
            Cursor allActivities;
            allActivities = ActivityDB.query(DATABASE_TABLE_Activity,new String[]{COLUMN_ACTIVITY_TABLE_id + " AS _id",COLUMN_ACTIVITY_TABLE_PseudoName,COLUMN_ACTIVITY_TABLE_isActive},null,null,null,null,COLUMN_ACTIVITY_TABLE_ActivityName);
            if (allActivities.moveToFirst())
            {
                ActivityDB.close();
                return allActivities;
            }
            ActivityDB.close();
            return null;
        }
        catch (Exception e)
        {
            Log.e(LOG_TAG_DB_ACT, e.getMessage());
            ActivityDB.close();
            return null;
        }
    }
}