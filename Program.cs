using System.Runtime.InteropServices;
using WebApplication11.Interfaces;
using WebApplication11.Providers;


var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var providerArg = args.FirstOrDefault()?.ToLower();

switch (providerArg)
{
    case "access":
        builder.Services.AddScoped<IDataProvider, AccessProvider>();
        break;
    case "sql":
        builder.Services.AddScoped<IDataProvider, SqlServerProvider>();
        break;
    case "mongodb":
        builder.Services.AddScoped<IDataProvider, MongoDbProvider>();
        break;
    default:
        builder.Services.AddScoped<IDataProvider, AccessProvider>();
        break;
}

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.MapControllers();

app.Run();
