from suricataparser import parse_file , parse_rule

def engine_alerting(enriched_event):
    # protocol = enriched_event.get("protocol")
    # tenant = enriched_event.get("tenant")
    
    # filepath = f"E:\NDR\rules\emerging-{protocol}.rules"
    rule = 'alert tcp $EXTERNAL_NET any -> $HOME_NET 8000 (msg:"ET MISC HP Web JetAdmin ExecuteFile admin access"; flow: to_server,established; content:"/plugins/framework/script/content.hts"; nocase; content:"ExecuteFile"; nocase; reference:bugtraq,10224; classtype:attempted-admin; sid:2001055; rev:6; metadata:created_at 2010_07_30, updated_at 2019_07_26;)'
    parsed_rule = parse_rule(rule)
    # rules_list = parse_file(filepath)
    print("Parsed Rule",parsed_rule.msg)

    
