require("RSclient")

rsc <- RS.connect(host = commandArgs(TRUE)[1], port = commandArgs(TRUE)[2])
#rsc <- RS.connect(host = "rserve.service.production", port = 6312)

expr = parse(commandArgs(TRUE)[3])
print(paste("Reading file", commandArgs(TRUE)[3]))
for (i in 1:length(expr)) {
    RS.eval(rsc, expr[[i]],lazy=F)
}
print(paste("There are", length(commandArgs(TRUE))-3, "arguments"))
command.list <- list(quote(main))
if(length(commandArgs(TRUE)) > 3){
    for (i in 4:length(commandArgs(TRUE))) {
        print(paste("appending argument", i-3, commandArgs(TRUE)[i]))
        command.list[[i-2]] <- commandArgs(TRUE)[i]
    }
}

RS.eval(rsc,as.call(command.list),lazy=F)
