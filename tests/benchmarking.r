
source("~/git/R/ESRfunctions.r")

stats <- dir('tests/benchmark_output', pattern = "*stats.csv$", full.names=TRUE)

params <- data.frame(
		ram = c(
				2, 1, 0.25, 0.5, .02, .25, 0.25, 5,
				4,8,16,32,64,128,256,
				4,8,16,32,64,128,256,
				4,8,16,32,64,128,256,
				4,8,16,32,64,128,256,
				4,8,16,32,64,128,256,
				4,8,16,32,64,128,256
		),
		cores = c(
				16, 16, 16, 16, 4, 4,
				8, 8, 8, 8, 8, 8, 8, 8, 8,
				16, 16, 16, 16, 16, 16, 16,
				4, 4, 4, 4, 4, 4, 4,
				8, 8, 8, 8, 8, 8, 8,
				16, 16, 16, 16, 16, 16, 16,
				16, 16, 16, 16, 16, 16, 16
		),
		x86 = c(
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE
		),
		v1 = c(
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE
		),
		ingesting = c(
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,
				TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE
		),
		row.names = c(
				"4xlarge_stats.csv", "4xlarge1g_stats.csv", "4xlarge256mb_stats.csv", "4xlarge500mb_stats.csv",
				"prod_stats.csv", "r5xlarge_stats.csv", "r6i2xlarge_stats.csv", "xxlarge-5gb_stats.csv",
				"2xlarge-4MB_stats.csv", "2xlarge-8MB_stats.csv","2xlarge-16MB_stats.csv","2xlarge-32MB_stats.csv",
				"2xlarge-64MB_stats.csv","2xlarge-128MB_stats.csv","2xlarge-256MB_stats.csv",
				"4xlarge-4MB_stats.csv", "4xlarge-8MB_stats.csv","4xlarge-16MB_stats.csv","4xlarge-32MB_stats.csv",
				"4xlarge-64MB_stats.csv","4xlarge-128MB_stats.csv","4xlarge-256MB_stats.csv",
				"xlarge-4MB_stats.csv", "xlarge-8MB_stats.csv","xlarge-16MB_stats.csv","xlarge-32MB_stats.csv",
				"xlarge-64MB_stats.csv","xlarge-128MB_stats.csv","xlarge-256MB_stats.csv",
				"2xlargeV2-4MB_stats.csv", "2xlargeV2-8MB_stats.csv","2xlargeV2-16MB_stats.csv","2xlargeV2-32MB_stats.csv",
				"2xlargeV2-64MB_stats.csv","2xlargeV2-128MB_stats.csv","2xlargeV2-256MB_stats.csv",
				"2xlargeARM-4MB_stats.csv", "2xlargeARM-8MB_stats.csv","2xlargeARM-16MB_stats.csv","2xlargeARM-32MB_stats.csv",
				"2xlargeARM-64MB_stats.csv","2xlargeARM-128MB_stats.csv","2xlargeARM-256MB_stats.csv",
				"2xlargeX86-4MB_stats.csv", "2xlargeX86-8MB_stats.csv","2xlargeX86-16MB_stats.csv","2xlargeX86-32MB_stats.csv",
				"2xlargeX86-64MB_stats.csv","2xlargeX86-128MB_stats.csv","2xlargeX86-256MB_stats.csv"
		)
)

x <- do.call(rbind, lapply(stats, function(path) {
		x <- read.csv(path)
		x$path <- basename(path)
		x[x$Name == 'Aggregated', ]
		#x[x$Name == 'v2/locations/:id', ]
		#x[x$Name == 'v2/latest/empty', ]
}))

x$cores <- params[x$path, "cores"]
x$ram <- params[x$path, "ram"]
x$x86 <- params[x$path, "x86"]
x$v1 <- params[x$path, "v1"]
x$ingesting <- params[x$path, "ingesting"]


x <- x[x$path != "prod_stats.csv", ]

x <- x[order(x$ram), ]

plot(Average.Response.Time ~ cores, x)
plot(Requests.s ~ cores, x)

plot(Average.Response.Time ~ ram, x)

ncores <- 16
plot(Requests.s ~ ram, subset(x, cores == ncores))
plot(Average.Response.Time ~ ram, subset(x, cores == ncores))
plot(Average.Response.Time ~ cores, subset(x, cores <= ncores), pch=cores, col=1)

plot(Average.Response.Time ~ ram, subset(x, cores <= ncores), pch=cores, col=1)
legend('topright', legend=unique(x$cores), pch=unique(x$cores), bty='n', ncol=3)

plot(Average.Response.Time ~ ram, subset(x, cores <= ncores), pch=19, col=as.numeric(1:nrow(x) %in% grep('V2', x$path))+1)
legend('topright', legend=c('V1', 'V2'), pch=19, col=1:2, bty='n', ncol=3)

plot(Average.Response.Time ~ ram, subset(x, cores == ncores), pch=19, col=as.numeric(1:nrow(x) %in% grep('ARM', x$path))+1)
legend('topright', legend=c('x86', 'ARM'), pch=19, col=1:2, bty='n', ncol=3)

plot(Average.Response.Time ~ ram, subset(x, cores == ncores), pch=19)
points(Average.Response.Time ~ ram, x[grep('ARM', x$path), ], pch=19, col='red')
legend('topright', legend=c('x86', 'ARM'), pch=19, col=1:2, bty='n', ncol=3)

plot(Average.Response.Time ~ ram, subset(x, cores == ncores & ingesting), pch=19, col=x$x86+1)
legend('topright', legend=c('x86', 'ARM'), pch=19, col=1:2, bty='n', ncol=3)


points(Average.Response.Time ~ ram, x[grep('ARM', x$path), ], pch=19, col='red')
legend('topright', legend=c('x86', 'ARM'), pch=19, col=1:2, bty='n', ncol=3)


plot(X50. ~ ram, x)
plot(X75. ~ ram, subset(x, cores == ncores))
plot(Request.Count ~ ram, subset(x, cores == ncores))
plot(Failure.Count ~ ram, subset(x, cores == ncores))
plot(ram ~ cores, x)


exporters <- dir('tests/benchmark_output', pattern = "*export_output*", full.names=TRUE)

params <- data.frame(
		ram = c(
				64, 0.128, .004, .004,
				.004, 1, 20, 40,
				5, 8, .004, .004
		),
		cores = c(
				16, 16, 4, 4,
				2, 8, 8, 8,
				8, 8, 4, 4
		),
		row.names = c(
				"4xlarge-wm64gb","4xlarge", "prod", "r5",
				"small", "xxlarge-wm1g", "xxlarge-wm20g", "xxlarge-wm40g",
				"xxlarge-wm5g", "xxlarge-wm8g", "xxlarge", "benchmark_export_output"
		)
)

x <- do.call(rbind, lapply(exporters, function(path) {
		x <- read.csv(path, quote="'")
		x$path <- basename(path)
		x$test <- gsub("benchmark_export_output_|.csv$", "", basename(path))
		return(x)
}))
x$cores = params[x$test,"cores"]
x$ram = params[x$test,"ram"]

boxplot(time_ms~cores, x)
plot(I(time_ms/1000)~jitter(ram, 10),x, log="y")
