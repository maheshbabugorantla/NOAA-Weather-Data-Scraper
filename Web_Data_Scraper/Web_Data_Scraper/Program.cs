/*  This is a WebScraper that is being used to scrape the Weather Data from the NOAA Weather Website.
 *
 *  I have designed this to automate the scraping of weather data in different regions from the NOAA Website
 *
 *  The Motivation behind the design of this Web-Scraper is to facilitate the understanding of the weather data by doing historical analysis for a specific region e.g. NAPA VALLEY, CA and to do predictive analytics on available data
 *  to help farmers understand the weather models and make informed decisions & precautions to protect their produce based on the weather data. For example, they can use these weather data models to understand how
 *  the weather can affect their crop production in a specific season.
 *
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using System.IO.Compression;

namespace Web_Data_Scraper
{
    class Program
    {
        static void Main(string[] args)
        {
                /*string year = args[0];
                string USAF = args[1];
                string WBAN = args[2];
                string file_name = args[1] + "-" + args[2] + "-" + args[0];*/

                // This is used to pull the Weather Data specific to a location based on Weather Station's WBAN and USAF Number.
                // In the Below Link WBAN Number is 724955, USAF Number is 93227 and Year is 2015.
                // Hence, if you are looking for the weather data of 2014 from the below mentioned weather station replace the below link with ""ftp://ftp.ncdc.noaa.gov/pub/data/noaa/2014/724955-93227-2014.gz"
                string ftp_path = "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/2015/724955-93227-2015.gz";

                string ftp_file_path = ftp_path; //ftp_path + year +"/" + file_name + ".gz";

                if (ftp_file_path.EndsWith(".txt") || ftp_file_path.EndsWith(".csv"))
                {
                    DownloadTextfile(ftp_file_path);
                }

                else if (ftp_file_path.EndsWith(".gz"))
                {
                   DownloadgzFile(ftp_file_path);
                }

                else
                {
                    Console.WriteLine("No File is Downloaded");
                }

                Console.Write("Press Enter to exit the Window: ");

                Console.Read();
        }

        /*  This is used rarely. This is used generally when we want to see if any updates are from a weather station */
        static void DownloadTextfile(string ftp_path)
        {
            // Get the object used to communicate with the server.
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(ftp_path); // "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv");

             if (request != null)
             {
                 request.Method = WebRequestMethods.Ftp.DownloadFile; // This is used to download the file.

                  //This assumes the data on the FTP site is public hence using anonymous login.
                 request.Credentials = new NetworkCredential("anonymous", "gorantla@purdue.edu");


                 FtpWebResponse response = (FtpWebResponse)request.GetResponse();

                 Stream responseStream = response.GetResponseStream();
                 //StreamReader reader = new StreamReader(responseStream);
                // Console.WriteLine(reader.ReadToEnd());

                 char[] delims = { '/' };
                 string[] strs = ftp_path.Split(delims);

                 FileStream file = File.Create(strs[strs.Length - 1]);

                 byte[] buffer = new byte[1 * 2048]; // This is used to read each line of the File

                 int read = 0;

                 while ((read = responseStream.Read(buffer, 0, buffer.Length)) > 0)
                 {
                     file.Write(buffer, 0, read);
                 }

                 Console.WriteLine("Download Complete, status {0}", response.StatusDescription);

                 file.Close();
                 responseStream.Close();
                 response.Close();
             }

             else
             {
                 Console.WriteLine("\n\n Unable to Make the request.. Please check the Network Connection and Try Again or May be the web link is not accessible anymore\n\n");
             }

             return;
        }

        static void DownloadgzFile(string ftp_path)
        {
            char[] delims = { '/' };
            string[] strs = ftp_path.Split(delims);

          //  try
           // {
                if (strs[strs.Length - 1].EndsWith(".gz"))
                {

                    // Downloading the .gz File from the FTP Site.
                    new WebClient().DownloadFile(ftp_path, strs[strs.Length - 1]);

                    Console.WriteLine("File with Name is downloaded " + strs[strs.Length - 1]);

                    // Decompressing the .gz file
                    Decompress_gz(strs[strs.Length - 1]);
                }

                else
                {
                    Console.WriteLine("Check If you are trying to really download a file with .gz extenstion");
                }

            //}

          //  catch
           // {
                    //Console.WriteLine(((FtpWebResponse)ex.Response).StatusDescription); // This Prints the Error Occured
          //          Console.WriteLine("Error Occured");
         //           return;
         //   }

            return;
        }

        static void Decompress_gz(string gz_file)
        {
            FileStream OriginalFileStream = File.OpenRead(gz_file); // This Returns a file stream to read.

            string extension = Path.GetExtension(gz_file); // This Gets the Extension of the File.
            string final_filename = gz_file.Substring(0, gz_file.Length - extension.Length); // Getting the File Name without the File Extension.

            final_filename = final_filename + ".txt";

            FileStream decompressedFileStream = File.Create(final_filename); // Creating a File to Store the Decompressed File

            GZipStream decompressionStream = new GZipStream(OriginalFileStream, CompressionMode.Decompress); // This sets the Decompression of the Original Compressed FileStream.

            decompressionStream.CopyTo(decompressedFileStream); // Copies the Decompressed File Stream to the desired file.

            // Closing all the File Handles
            decompressionStream.Close();
            decompressedFileStream.Close();
            OriginalFileStream.Close();

            // Parsing the Decompressed File.
            Parse_File(final_filename);
            Console.WriteLine("Decompressed File Name is " + final_filename);

            return;
        }

        static void Parse_File(string file_name)
        {
            Console.WriteLine("Opening the Files");
            string[] lines = File.ReadAllLines(file_name);

            string file_without_ext = Path.GetFileNameWithoutExtension(file_name);
            StreamWriter file_write = new StreamWriter(file_without_ext + "_Parsed.csv", false);

            int index = 1;
            Console.WriteLine("Writing to File");

            file_write.Write("S.No,USAF(C),WBAN(C),Date(C),Time(C),Latitude(C),Longitude(C),Wind_Dir_Angle(M),Wind_Gust(N/A),Wind_Speed_Rate(M),Solar Radiation(M),Air_Obs_Temp(Centigrade)(M),Atmos_Press_Rel_MSL(A),Relative_Humidity (A),Precipitation(mm)(A)\n");

            foreach (string line in lines)
            {
                file_write.Write(index);
                file_write.Write(",");

                // ****************** CONTROL DATA SECTION *******************************
                var val1 = line.Substring(4, 6);
                file_write.Write(val1); // USAF Number of the Weather Station.
                file_write.Write(",");
                file_write.Write(line.Substring(10, 5)); // WBAN Number of the Weather Station.
                file_write.Write(",");
                var date = line.Substring(15, 8);
                date = date.Substring(0, 4) + "-" + date.Substring(4, 2) + "-" + date.Substring(6, 2);
                file_write.Write(date); // Year-Month-Day (YYYYMMDD Format)
                file_write.Write(",");
                var time = Convert.ToString(line.Substring(23, 4));
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                file_write.Write(time); // Hours-Minutes (HHMM)
                file_write.Write(",");

                // Latitude Co-Ordinate
                var Latitude = Convert.ToString(line.Substring(28, 6)); // Getting the Latitude

                if (Latitude != "+99999")
                {
                    var sign_lat = Convert.ToString(Latitude.Substring(0, 1));
                    Latitude = Convert.ToString(Latitude.Substring(1, 5));
                    double conv_lat = (Int64.Parse(Latitude));
                    conv_lat = (Double)(conv_lat / Math.Pow(10, 3));
                    file_write.Write(sign_lat + conv_lat.ToString());
                }

                else
                {
                    file_write.Write("NULL"); // Which Means that the Latitude is not found.
                }

                file_write.Write(",");

                // Longitude Co-Ordinate
                var Longitude = line.Substring(34, 7);

                if (Longitude != "+999999")
                {
                    var sign_long = Convert.ToString(Longitude.Substring(0, 1));
                    Longitude = Convert.ToString(Longitude.Substring(1, 5));
                    double conv_long = (Int64.Parse(Longitude));
                    conv_long = (Double)(conv_long / Math.Pow(10, 3));
                    file_write.Write(sign_long + conv_long.ToString());
                }

                else
                {
                    file_write.Write("NULL"); // Which Means that the Longitude is not found.
                }

                file_write.Write(","); // Space Separation

                // Geo-Physical Surface Observation Type.  ( COMMENTED - OUT )
               /* var report_type = Convert.ToString(line.Substring(41, 5));

                if (report_type != "99999")
                {
                    file_write.Write(report_type);
                    file_write.Write(","); // Space Separation
                }

                else
                {
                    file_write.Write("NULL"); // Missing
                    file_write.Write(","); // Space Separation
                }

                // Elevation of a Geo-Physical Point Observation rel. to Mean Sea Level.
                var elevation_dim = Convert.ToString(line.Substring(46, 5));

                if (elevation_dim != "+9999")
                {
                    file_write.Write(elevation_dim);
                    file_write.Write(","); // Space Separation
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(","); // Space Separation
                }

                // Call Letter Identifier for a fixed Weather Station.
                var fixed_stat_call_id = Convert.ToString(line.Substring(51, 5));

                if (fixed_stat_call_id != "99999")
                {
                    file_write.Write(fixed_stat_call_id);
                    file_write.Write(","); // Space Separation
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(","); // Space Separation
                }

                // Meteorological Point Observation Quality Control Process Name
                var quality_control_process = line.Substring(56, 4);
                //Console.WriteLine(quality_control_process);
                file_write.Write(Convert.ToString(quality_control_process));
                file_write.Write(",");*/


                // ******************** MANDATORY DATA SECTION ************************************

                // Wind Observation Direction Angle
                var wind_dir_angle = Convert.ToString(line.Substring(60, 3)); // Units are Angular Degrees

                if (wind_dir_angle != "999")
                {
                    file_write.Write(wind_dir_angle);
                    file_write.Write(",");
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

            /*    // Wind Observation Direction Quality Code ( COMMENTED - OUT )
                var wind_dir_QCode = Convert.ToString(line.Substring(63, 1));
                file_write.Write(wind_dir_QCode);
                file_write.Write(",");

                // Wind Observation Type Code
                var wind_obs_code = Convert.ToString(line.Substring(64, 1));
                file_write.Write(wind_obs_code);
                file_write.Write(","); */

                // Wind Gust
                var wind_gust = "NULL";
                file_write.Write(wind_gust);
                file_write.Write(",");


                // Wind Observation Speed Rate (Meters / Second) and SCALING FACTOR: 10
                var wind_obs_speed_rate = Convert.ToString(line.Substring(65, 4));

                if (wind_obs_speed_rate != "9999")
                {
                    file_write.Write(Convert.ToDouble(wind_obs_speed_rate) / 10);
                    file_write.Write(",");
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

             /*   // Wind Observation Speed Quality Code
                var wind_obs_speed_QCode = Convert.ToString(line.Substring(69, 1));
                file_write.Write(wind_obs_speed_QCode);
                file_write.Write(",");

                // Sky Condition Observation Ceiling height dimension. UNITS: Meters
                var sky_obs_ceiling_height = Convert.ToString(line.Substring(70, 5));

                if (sky_obs_ceiling_height != "99999")
                {
                    file_write.Write(sky_obs_ceiling_height);

                    if (sky_obs_ceiling_height == "22000")
                    {
                        file_write.Write(" (UnLimited)");
                    }

                    file_write.Write(",");
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

                // Sky Condition Observation Ceiling Quality Code.
                var sky_obs_ceil_QCode = line.Substring(75, 1);
                file_write.Write(sky_obs_ceiling_height);
                file_write.Write(",");

                // Sky Condition Observation Ceiling Determination(Method of Determination) Code.
                var sky_obs_ceil_method_QCode = line.Substring(76, 1);
                file_write.Write(sky_obs_ceil_method_QCode);
                file_write.Write(",");

                // Sky Condition Observation CAVOK Code.
                var sky_obs_CAVOK = line.Substring(77, 1);
                file_write.Write(sky_obs_CAVOK);
                file_write.Write(",");

                // Visibility Observation Distance Dimension.
                var vis_obs_dist_dim = line.Substring(78, 6);

                if (Convert.ToInt64(vis_obs_dist_dim) <= 160000)
                {
                    file_write.Write(vis_obs_dist_dim);
                    file_write.Write(",");
                }

                else
                {
                    file_write.Write(160000);
                    file_write.Write(",");
                }

                // Visibility Observation Distance Quality Code
                var vis_obs_dist_QCode = line.Substring(84, 1);
                file_write.Write(vis_obs_dist_QCode);
                file_write.Write(",");

                // Visibility Observation Variability Code
                var vis_obs_var_Code = line.Substring(85, 1);

                if (vis_obs_var_Code != "9")
                {
                    file_write.Write(vis_obs_var_Code);
                    file_write.Write(",");
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

                // Visibility Observation Quality Variability Code.
                var vis_obs_QVar_Code = line.Substring(86, 1);
                file_write.Write(vis_obs_QVar_Code);
                file_write.Write(",");*/

                // Solar Radiation
                var solar_radiation = "NULL";
                file_write.Write(solar_radiation);
                file_write.Write(",");

                // Air Temperature Observation Air Temperature in Centigrade
                var air_temp_obs = line.Substring(87, 5);

                var Temp = Convert.ToDouble(air_temp_obs.Substring(1, 4)) / 10;

                if (air_temp_obs != "+9999")
                {
                    var sym = air_temp_obs.Substring(0, 1);
                    file_write.Write(sym + Temp);
                    file_write.Write(",");
                }


                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

              /*  // Air Temperature Observation Quality Code
                var air_temp_obs_QCode = line.Substring(92, 1);
                file_write.Write(air_temp_obs_QCode);
                file_write.Write(",");

                // Air Temperature Observation Dew Point Temperature UNITS: Degree Centigrade
                var air_temp_obs_dew = line.Substring(93, 5);

                if (air_temp_obs_dew != "+9999")
                {
                    var sym = air_temp_obs_dew.Substring(0, 1);
                    file_write.Write(sym + Convert.ToDouble(air_temp_obs_dew.Substring(1, 4)) / 10);
                    file_write.Write(",");
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

                // Air Temperature Observation Dew Point Quality Code
                var air_temp_obs_dew_QCode = line.Substring(98, 1);
                file_write.Write(air_temp_obs_dew_QCode);
                file_write.Write(",");*/

                // Atmospheric Pressure Observation Relative to Mean Sea Level Pressure UNITS: Kilo-Pascals (Original Readings are in Hecto-Pascals)
                var air_pressure_rel_MSL = line.Substring(99, 5);

                if (air_pressure_rel_MSL != "99999")
                {
                    file_write.Write(Convert.ToDouble(air_pressure_rel_MSL) / 100);
                    file_write.Write(",");

                    var air_pressure_conv = 0.611 * Math.Exp(Convert.ToDouble(17.502 * Temp) / (240.97 + Temp));

                    Console.WriteLine(air_pressure_conv);

                    var Relative_Humidity = (Convert.ToDouble(air_pressure_rel_MSL) / 100) / air_pressure_conv;

                    file_write.Write(Relative_Humidity);

                    file_write.Write(",");
                }

                else
                {
                    file_write.Write("NULL,NULL,");
                }

             /*   // Atmospheric Pressure Observation Sea Level Pressure Quality Code.
                var air_pressure_QCode = line.Substring(104, 1);
                file_write.Write(air_pressure_QCode);
                file_write.Write(",");*/

                // ************************** ADDITIONAL DATA SECTION ***************************************

                // Searching for the Precipitation from the Additional Data Section

                string Add_String = line.Substring(105, (line.Length - 105));

                int index_AA1 = Add_String.IndexOf("AA1");
                int index_AA2 = Add_String.IndexOf("AA2");

                if(index_AA1 != -1)
                {
                    //Console.WriteLine("Index of AA1 is " +index_AA1);
                    string str_AA1 = Add_String.Substring(index_AA1 + 5, 4);

                    if(str_AA1 != "9999")
                    {
                        string AA1 = Convert.ToString(Convert.ToDouble(str_AA1) / 10); // UNITS: Millimeters , SCALING FACTOR: 10
                        file_write.Write(AA1);
                        file_write.Write(",");
                    }

                    else
                    {
                        file_write.Write("NULL");
                        file_write.Write(",");
                    }
                }

                else if(index_AA2 != -1)
                {
                    //Console.WriteLine("Index of AA2 is " + index_AA2);
                    string str_AA2 = Add_String.Substring(index_AA2 + 5, 4);

                    if (str_AA2 != "9999")
                    {
                        string AA2 = Convert.ToString(Convert.ToDouble(str_AA2) / 10); // UNITS: Millimeters , SCALING FACTOR: 10
                        file_write.Write(AA2);
                        file_write.Write(",");
                    }

                    else
                    {
                        file_write.Write("NULL");
                        file_write.Write(",");
                    }
                }

                else
                {
                    file_write.Write("NULL");
                    file_write.Write(",");
                }

                file_write.Write("\n"); // New Line Separation
                index += 1;
            }

            Console.WriteLine("Closing the Files");
            //file_read.Close();
            file_write.Close();

            return;

        }
    }
}
