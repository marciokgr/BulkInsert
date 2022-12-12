using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using System.Data;
using Bogus;

namespace BulkInsert
{
    class Program
    {        

        static void Main(string[] args)
        {
            try
            {

                var dataInicio = DateTime.Now;

                var configuration = new ConfigurationBuilder().AddJsonFile($"appsettings.json");

                var config = configuration.Build();

                var connectionString = config["ConnectionStrings:defaultConnection"];

                Program objProgram = new Program();

                DataTable dtTable = new DataTable();

                dtTable.Columns.Add("IDBULK");
                dtTable.Columns.Add("NM_NOME");
                dtTable.Columns.Add("NM_ENDERECO");

                Console.WriteLine("Iniciando loop de geração de dados " + DateTime.Now);

                var dataInicioGeracao = DateTime.Now;

                int quantidadeRegistros = 1000000 + 1; //quantidade registros

                //gerando dados fake para inserção
                var faker = new Faker();

                for (int i = 1; i < quantidadeRegistros; i++)
                {
                    
                    DataRow linha = dtTable.NewRow();
                    linha["IDBULK"] = i ;
                    linha["NM_NOME"] = faker.Name.FullName(); //nome fake
                    linha["NM_ENDERECO"] = faker.Address.StreetName(); //endereço fake

                    dtTable.Rows.Add(linha);

                }

                TimeSpan spanGeracao = DateTime.Now - dataInicioGeracao;
                Console.WriteLine("Tempo total geração dos dados : " + spanGeracao.ToString(@"dd\.hh\:mm\:ss"));

                Console.WriteLine("Dados gerados " + DateTime.Now);


                var dataInicioInsercao = DateTime.Now;
                Console.WriteLine("Iniciando inserção " + dataInicioInsercao);


                using (var connection = new OracleConnection(connectionString))
                {
                    connection.Open();
                    Console.WriteLine("Conexao aberta " + DateTime.Now);

                    //Fazendo bulkinsert oracle
                    objProgram.SaveUsingOracleBulkCopy(dtTable, connection);

                    connection.Close();
                    Console.WriteLine("Conexao fechada " + DateTime.Now);

                }

                Console.WriteLine("Tempo total inserção dos dados : " + spanGeracao.ToString(@"dd\.hh\:mm\:ss"));


                TimeSpan span = DateTime.Now - dataInicio;
                Console.WriteLine("Tempo total execução de "+ quantidadeRegistros  + " linhas : "+ span.ToString(@"dd\.hh\:mm\:ss"));

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        //Inserindo todos o registros
        public void SaveUsingOracleBulkCopy(DataTable dtTable, OracleConnection connection)
        {
            try
            {
                    int[] ids = new int[dtTable.Rows.Count];
                    string[] nomes = new string[dtTable.Rows.Count];
                    string[] enderecos = new string[dtTable.Rows.Count];

                    Console.WriteLine("Iniciando formatação dos dados " + DateTime.Now);

                    for (int i = 0; i < dtTable.Rows.Count; i++)
                    {
                        ids[i] = Convert.ToInt32(dtTable.Rows[i]["IDBULK"]);
                        nomes[i] = Convert.ToString(dtTable.Rows[i]["NM_NOME"]);
                        enderecos[i] = Convert.ToString(dtTable.Rows[i]["NM_ENDERECO"]);
                    }

                    Console.WriteLine("Fim formatação do dados " + DateTime.Now);

                    OracleParameter id = new OracleParameter();
                    id.OracleDbType = OracleDbType.Byte;
                    id.Value = ids;

                    OracleParameter nome = new OracleParameter();
                    nome.OracleDbType = OracleDbType.Varchar2;
                    nome.Value = nomes;

                    OracleParameter endereco = new OracleParameter();
                    endereco.OracleDbType = OracleDbType.Varchar2;
                    endereco.Value = enderecos;

                    OracleCommand comando = connection.CreateCommand();
                    comando.CommandText = "INSERT INTO TECNOLOGIA.TB_BULKINSERT_TEST (IDBULK, NM_NOME, NM_ENDERECO) VALUES (:1, :2, :3)";
                    comando.ArrayBindCount = ids.Length;
                    comando.Parameters.Add(id);
                    comando.Parameters.Add(nome);
                    comando.Parameters.Add(endereco);

                    Console.WriteLine("Iniciando inserção de  "+ ids.Length + " linhas " + DateTime.Now);

                    comando.ExecuteNonQuery();

                    Console.WriteLine("Fim inserção de  " + ids.Length + " linhas " + DateTime.Now);


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }      

    }
}