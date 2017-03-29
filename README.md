# GenstageExample

## Issues

* If using BroadcastDispatcher then events send to all consumers of a particular type
* Can be solved using a ConsumerSupervisor, but this cannot produce events normally

* Can produce only events?

* Send external events from producer
  * Use `GenStage.call(GenStageExample.JobProducer,{:notify, {:summary, 1}})` or similar

* Tell consumers to stop working
  * Use `GenStage.sync_notify(GenStageExample.JobProducer, :back_off)` or similar
  * Handle this in a `handl_info()` callback
  * Can send a similar message to tell them to start again
