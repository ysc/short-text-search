
Utils = function() {
    var object = {
        logEnabled: false,
        minuteSnap: function(date){
            return moment(date, "YYYY-MM-DD HH:mm:ss").format("YYYYMMDDHHmm");
        },
        log: function(info){
            if(Utils.logEnabled){
                console.log(info);
            }
        },
        getTimeDiff: function(startDate, endDate) {
            var startMoment = moment(startDate, "YYYY-MM-DD HH:mm:ss");
            var endMoment = moment(endDate, "YYYY-MM-DD HH:mm:ss");
            var diff = endMoment.diff(startMoment);
            return diff;
        },
        getTimeSpanDes: function(startDate, endDate) {
            var startMoment = moment(startDate, "YYYY-MM-DD HH:mm:ss");
            var endMoment = moment(endDate, "YYYY-MM-DD HH:mm:ss");
            var diff = endMoment.diff(startMoment);
            var diffDesc = Utils.getTimeDes(diff);
            return diffDesc;
        },
        getTimeDes: function(ms) {
            //处理参数为NULL的情况
            if(ms == undefined){
                return "";
            }
            var minus = false;
            if(ms < 0){
                minus = true;
                ms = -ms;
            }
            var ss = 1000;
            var mi = ss * 60;
            var hh = mi * 60;
            var dd = hh * 24;

            var day = parseInt(ms / dd);
            var hour = parseInt((ms - day * dd) / hh);
            var minute = parseInt((ms - day * dd - hour * hh) / mi);
            var second = parseInt((ms - day * dd - hour * hh - minute * mi) / ss);
            var milliSecond = ms - day * dd - hour * hh - minute * mi - second * ss;

            var str = "";
            if(day > 0){
                str+=day+"天,";
            }
            if(hour > 0){
                str+=hour+"小时,";
            }
            if(minute > 0){
                str+=minute+"分钟,";
            }
            if(second > 0){
                str+=second+"秒,";
            }
            if(milliSecond > 0){
                str+=milliSecond+"毫秒,";
            }
            if(str.length > 0){
                str=str.slice(0, str.length-1);
            }

            if(minus){
                return "-"+str;
            }

            return str;
        }
    }
    return object;
} ();