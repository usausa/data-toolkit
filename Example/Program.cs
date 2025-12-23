using Example;

using Smart.CommandLine.Hosting;

var builder = CommandHost.CreateBuilder(args);
builder.ConfigureCommands(commands =>
{
    commands.ConfigureRootCommand(root =>
    {
        root.WithDescription("Example");
    });

    commands.AddCommands();
});

var host = builder.Build();
var exitCode = await host.RunAsync();
#if DEBUG
Console.WriteLine($"ExitCode: {exitCode}");
Console.ReadLine();
#endif
return exitCode;
