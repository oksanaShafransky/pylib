require("RSclient")

#Init rserve client
rsc <- RS.connect(host = commandArgs(TRUE)[1], port = commandArgs(TRUE)[2])

#Read expressions from R file and eval on Rserve
expressions.list = parse(commandArgs(TRUE)[3])
print(paste("Reading file", commandArgs(TRUE)[3]))
for (i in 1:length(expressions.list)) {
    RS.eval(rsc, expressions.list[[i]],lazy=F)
}

#Create main call with arguments
mandatory.parameters.count <- 3
print(paste("There are", length(commandArgs(TRUE)) - mandatory.parameters.count, "arguments"))
command.list <- list(quote(main))
first.arg.index <- mandatory.parameters.count+1
last.arg.index <- length(commandArgs(TRUE))
command.list.index = 2
if(length(commandArgs(TRUE)) > mandatory.parameters.count){
    for (i in first.arg.index:last.arg.index) {
        print(paste("appending argument", i-mandatory.parameters.count, commandArgs(TRUE)[i]))
        command.list[[command.list.index]] <- commandArgs(TRUE)[i]
        command.list.index <- command.list.index+1
    }
}

res <- try(RS.eval(rsc,as.call(command.list),lazy=F))
if(class(res) == "try-error"){
    RS.eval(rsc, traceback())
}else{
    res
}