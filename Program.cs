using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;
using System;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using DapperProj.Models;
using System.Collections.Generic;
using System.Linq;

namespace DapperProj
{
    class Program
    {
        static void Main(string[] args)
        {

            var category = new Category();
            category.Id = Guid.NewGuid();
            category.Title = "Amazon AWS";
            category.Url = "amazon";
            category.Summary = "AWS Cloud";
            category.Order = 1;
            category.Description = "AWS Services";
            category.Featured = false;

            var category2 = new Category();
            category2.Id = Guid.NewGuid();
            category2.Title = "Amazon AWS 2";
            category2.Url = "amazon 2";
            category2.Summary = "AWS Cloud 2";
            category2.Order = 2;
            category2.Description = "AWS Services 2";
            category2.Featured = false;

            var updateCategory = new Category();
            updateCategory.Id = category.Id;
            updateCategory.Title = "Amazon AWS 333";

            using (
                var con =
                    new SqlConnection(@"Data Source=desktop-vf2hide\sqlexpress;Initial Catalog=balta;Integrated Security=True;Connect Timeout=30")
            )
            {

                var deletedStudentsCount = ExecuteProcedureSpDeleteStudent(con, "d717423c-1791-46d7-8fb3-dc2938767437");
                Console.WriteLine($"{deletedStudentsCount} deleted rows");

                clearCategoryTable(con);

                var insertedRows = InsertCategory(con, category, category2);
                Console.WriteLine($"{insertedRows} inserted rows");

                var updatedRows = UpdateCategory(con, updateCategory);
                Console.WriteLine($"{updatedRows} updated rows");

                ListCategories(con);

                ExecuteProcedureSpDeleteStudent(con, "d717423c-1791-46d7-8fb3-dc2938767437");   
                ExecuteProcedureSpGetCoursesByCategory(con, new Guid("746e2cb2-d120-49ab-b9d2-761dc37a68aa"));

                Console.WriteLine($"New Id returned {InsertCategoryScalar<Guid>(con, category)}");               

                ReadViewCourses(con);

                OneToOne(con);
                OneToMany(con);
                QueryMultiple(con);
                SelectIn(con);
                SelectLike(con);
                Console.WriteLine($"Affected rows on transaction {InsertCategoryTransaction(con, category)}");

            }


            static void ListCategories(SqlConnection con)
            {
                var categories =
                    con.Query<Category>("SELECT [Id], [Title] FROM [Category]");

                foreach (var item in categories)
                {
                    Console.WriteLine($"{item.Id} - {item.Title}");
                }
            }


            static int InsertCategory(SqlConnection con, Category category, Category category2)
            {

                var insertSQL =
                    @"INSERT INTO [Category] 
                        VALUES 
                        (
                                @Id, 
                                @Title, 
                                @Url, 
                                @Summary, 
                                @Order, 
                                @Description, 
                                @Featured
                        )";

                return con
                    .Execute(insertSQL,
                    new[] {
                            new {
                                category.Id,
                                category.Title,
                                category.Url,
                                category.Summary,
                                category.Order,
                                category.Description,
                                category.Featured
                            },
                                new {
                                category2.Id,
                                category2.Title,
                                category2.Url,
                                category2.Summary,
                                category2.Order,
                                category2.Description,
                                category2.Featured
                            }
                    });
            }

            static Guid InsertCategoryScalar<Guid>(SqlConnection con, Category category)
            {

                var insertSQL =
                    @"INSERT INTO [Category] 
                        OUTPUT inserted.[Id]
                        VALUES 
                        (
                                NEWID(),
                                @Title, 
                                @Url, 
                                @Summary, 
                                @Order, 
                                @Description, 
                                @Featured
                        )";

                return con
                    .ExecuteScalar<Guid>(insertSQL,
                        new {
                            category.Title,
                            category.Url,
                            category.Summary,
                            category.Order,
                            category.Description,
                            category.Featured
                        }
                    );
            }

            static int UpdateCategory(SqlConnection con, Category category)
            {
                var updateSQL =
                    @"UPDATE [Category] 
                            SET [Title] = @Title
                            WHERE [Id] = @Id";

                return con
                    .Execute(updateSQL,
                    new
                    {
                        category.Id,
                        category.Title
                    });
            }

            static void clearCategoryTable(SqlConnection con)
            {

                /*
                var deleteAllSQL =
                    @"DELETE FROM[Category]";
                con.Execute(deleteAllSQL);
                */

            }

            static int ExecuteProcedureSpDeleteStudent(SqlConnection con, string guidId ){
                var procedure = "[spDeleteStudent]";
                var parameter = new { StudentId = guidId };
                return con.Execute(procedure, parameter, commandType: CommandType.StoredProcedure);
            }

            static void ExecuteProcedureSpGetCoursesByCategory(SqlConnection con, Guid guidCategoryId ){
                
                var procedure = "[spGetCoursesByCategory]";
                var parameter = new { CategoryId = guidCategoryId };
                var courses = con.Query(procedure, parameter, commandType: CommandType.StoredProcedure);

                foreach (var course in courses) {
                    Console.WriteLine($" Course Id: {course.Id}");
                }

            }

            static void ReadViewCourses(SqlConnection con){
                var courses =
                    con.Query<Category>("SELECT [Id], [Title] FROM [vwCourses]");

                foreach (var item in courses)
                {
                    Console.WriteLine($"View Courses {item.Id} - {item.Title}");
                }                
            }

            static void OneToOne(SqlConnection con){
                var sql = @" SELECT * FROM [CareerItem] INNER JOIN [Course] ON [CareerItem].[CourseId] = [Course].[Id]";
                var items =
                    con.Query<CareerItem, Course, CareerItem>(sql, (careerItem, course) => {
                        careerItem.Course = course;
                        return careerItem;
                    }, splitOn: "Id");

                foreach (var item in items)
                {
                    Console.WriteLine($"OneToOne {item.Course.Title}");
                }                
            }

            static void OneToMany(SqlConnection con){

                var sql = @"SELECT 
                                [Career].[Id],
                                [Career].[Title],
                                [CareerItem].[CareerId],
                                [CareerItem].[Title]
                          FROM [Career] 
                          INNER JOIN [CareerItem] ON [CareerItem].[CareerId] = [Career].[Id]
                          ORDER BY [Career].[Title]";

                var careersList = new List<Career>();

                var careers =
                    con.Query<Career, CareerItem, Career>(sql, (career, careerItem) => {


                        var existCareer = careersList.Where(x => x.Id == career.Id).FirstOrDefault();

                        if (existCareer == null){
                            existCareer = career;
                            existCareer.Items.Add(careerItem);
                            careersList.Add(existCareer);
                        } else {
                            existCareer.Items.Add(careerItem);
                        }

                        return career;

                    }, splitOn: "CareerId");

                foreach (var career in careers)
                {
                    Console.WriteLine($"OneToMany Career {career.Title}");
                    foreach (var item in career.Items)
                    {
                        Console.WriteLine($"OneToMany CareerItem {item.Title}");
                    }
                }                
            }

            static void QueryMultiple(SqlConnection con) {

                var sql = "SELECT * FROM [Category]; SELECT * FROM [Course]";

                using (var multi = con.QueryMultiple(sql)){
                   
                    var categories = multi.Read<Category>();
                    var courses = multi.Read<Course>();

                    foreach (var item in categories)
                    {
                        Console.WriteLine($"QueryMultiple Category {item.Title}");
                    }

                    foreach (var item in courses)
                    {
                        Console.WriteLine($"QueryMultiple Courses {item.Title}");
                    }

                };
            }

            static void SelectIn(SqlConnection con) {

                var sql = "SELECT * FROM [Category] WHERE [Category].[Id] IN @Id";

                var items = con.Query<Category>(sql, new
                {
                   Id = new[] {
                        "41D8DF5A-39AA-49C7-9116-0B10ECC9DF78",
                        "F51DC1C1-8566-4201-960F-0C2FF12D791D"
                    }
                });

                foreach (var item in items){
                    Console.WriteLine($" SelectIn - {item.Title}");
                }
                
            }

            static void SelectLike(SqlConnection con) {

                var sql = "SELECT * FROM [Category] WHERE [Category].[Title] Like @Exp";

                var items = con.Query<Category>(sql, new
                {
                    Exp = "%AWS%"
                });

                foreach (var item in items){
                    Console.WriteLine($" SelectLike - {item.Title}");
                }
                
            }

            static int InsertCategoryTransaction(SqlConnection con, Category category)
            {

                var insertSQL =
                    @"INSERT INTO [Category] 
                        VALUES 
                        (
                                NEWID(), 
                                @Title, 
                                @Url, 
                                @Summary, 
                                @Order, 
                                @Description, 
                                @Featured
                        )";

                con.Open();

                using (var transaction = con.BeginTransaction()){
                                       
                    var affectedRows = con.Execute(insertSQL,                    
                            new {
                                category.Title,
                                category.Url,
                                category.Summary,
                                category.Order,
                                category.Description,
                                category.Featured
                            }, transaction);

                    //transaction.Commit();                    
                    transaction.Rollback();

                    return affectedRows;
                }
            }

        }
    }
}
