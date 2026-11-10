package synthesizer

import (
 "encoding/json"
 "testing"
 "time"

 "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
 "gopkg.in/yaml.v3"
)

func TestSubscriptionFallbackSerializationSynthesisReload(t *testing.T) {
 for _,enabled:=range []bool{false,true}{
  cfg:=config.Config{ClaudeKey:[]config.ClaudeKey{{APIKey:"synthetic-claude",SubscriptionFallback:&config.SubscriptionFallbackConfig{AuthID:"parent-claude",Enabled:enabled}}},CodexKey:[]config.CodexKey{{APIKey:"synthetic-codex",SubscriptionFallback:&config.SubscriptionFallbackConfig{AuthID:"parent-codex",Enabled:enabled}}}}
  for _,format:=range []string{"yaml","json"}{
   var data []byte;var err error;var restored config.Config
   if format=="yaml"{data,err=yaml.Marshal(cfg);if err==nil{err=yaml.Unmarshal(data,&restored)}}else{data,err=json.Marshal(cfg);if err==nil{err=json.Unmarshal(data,&restored)}}
   if err!=nil{t.Fatal(err)}
   synth:=NewConfigSynthesizer();ctx:=&SynthesisContext{Config:&restored,Now:time.Now(),IDGenerator:NewStableIDGenerator()}
   auths,err:=synth.Synthesize(ctx);if err!=nil||len(auths)!=2{t.Fatalf("synthesis: %v %d",err,len(auths))}
   for _,a:=range auths {want:="false";if enabled{want="true"};if a.Attributes["subscription_fallback_enabled"]!=want||a.Attributes["subscription_fallback_auth_id"]!="parent-"+a.Provider{t.Fatalf("lost fallback association for %s",a.Provider)}}
   restored.ClaudeKey[0].SubscriptionFallback=nil;restored.CodexKey[0].SubscriptionFallback=nil
   auths,err=synth.Synthesize(ctx);if err!=nil{t.Fatal(err)}
   for _,a:=range auths{if _,present:=a.Attributes["subscription_fallback_enabled"];present{t.Fatal("reload retained removed fallback marker")}}
  }
 }
}
